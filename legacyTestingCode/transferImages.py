#!/usr/bin/env python3
import os
import time
import json
import hashlib
from typing import Iterator
from xmlrpc import client

import tos
import requests

from volcengine.auth.SignerV4 import SignerV4
from volcengine.Credentials import Credentials
from volcengine.base.Request import Request

# ----------------------------
# Config
# ----------------------------
from dotenv import load_dotenv
import uuid

doc_id = str(uuid.uuid4())
load_dotenv()
AK = os.getenv("AK")
SK = os.getenv("SK")

REGION = "ap-southeast-1"

# VikingDB (Data Plane) endpoint for Johor
VIKINGDB_HOST = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"
VIKINGDB_SERVICE = "vikingdb"
COLLECTION_NAME = "ImageCollection"

# TOS bucket
BUCKET = "bucketforvectordbdemo"

# IMPORTANT:
# - For TOS Python SDK, use TOS endpoint (tos-...), NOT S3 endpoint (tos-s3-...)
TOS_ENDPOINT = "tos-ap-southeast-1.bytepluses.com"

# Folder structure
BASE_PREFIX = "data/"
# SUBFOLDERS = ["bird", "airplane", "car", "cat", "deer", "dog", "horse", "monkey", "ship", "truck"]
SUBFOLDERS = ["truck"]

PREFIXES = [f"{BASE_PREFIX}{name}/" for name in SUBFOLDERS]

# Optional: be gentle on APIs
SLEEP_SECONDS = 0.01

# ----------------------------
# Helpers
# ----------------------------
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def list_bucket_keys(prefix: str) -> Iterator[str]:
    """
    Lists object keys in TOS under a prefix using the official TOS SDK.
    """
    tos_client = tos.TosClientV2(AK, SK, TOS_ENDPOINT, REGION)

    token = None
    while True:
        resp = tos_client.list_objects_type2(
            bucket=BUCKET,
            prefix=prefix,
            max_keys=1000,
            continuation_token=token
        )

        for obj in (resp.contents or []):
            key = obj.key
            if key and not key.endswith("/"):
                yield key

        if getattr(resp, "is_truncated", False):
            token = getattr(resp, "next_continuation_token", None)
        else:
            break
def make_unique_string_id_from_key(key: str) -> str:
    # Deterministic, unique per object key (recommended)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()
class VikingDBDataPlaneClient:
    """
    VikingDB Data Plane client using Volcengine SignerV4.
    """
    def __init__(self, ak: str, sk: str, host: str, region: str):
        self.ak = ak
        self.sk = sk
        self.host = host
        self.region = region
        self.session = requests.Session()

    def _prepare(self, method: str, path: str, body: dict):
        r = Request()
        r.set_shema("https")
        r.set_method(method.upper())
        r.set_host(self.host)
        r.set_path(path)
        r.set_headers({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Host": self.host,
        })
        r.set_body(json.dumps(body))

        creds = Credentials(self.ak, self.sk, VIKINGDB_SERVICE, self.region)
        SignerV4.sign(r, creds)
        return r

    def upsert_one(self, data_row: dict):
        path = "/api/vikingdb/data/upsert"
        body = {
            "collection_name": COLLECTION_NAME,
            "data": [data_row],  # 1 row per request
            # DO NOT include "async" for vectorized collections
        }

        req = self._prepare("POST", path, body)
        url = f"https://{self.host}{path}"
        resp = self.session.post(url, headers=req.headers, data=req.body, timeout=30)
        return resp.status_code, resp.text


# ----------------------------
# Main
# ----------------------------
def make_id_int64(key: str) -> int:
    # 63-bit positive int derived from key (stable + fits int64)
    return int(hashlib.sha1(key.encode("utf-8")).hexdigest()[:15], 16)  # 60 bits

def main():
    client = VikingDBDataPlaneClient(AK, SK, VIKINGDB_HOST, REGION)

    total = 0
    for prefix in PREFIXES:
        print(f"\n== Prefix: {prefix}")

        count_in_prefix = 0
        for key in list_bucket_keys(prefix):
            doc_id = make_unique_string_id_from_key(key)
            row = {
                "id": doc_id,
                "image": f"tos://{BUCKET}/{key}",
                "created_at": int(time.time()),
            }

            status, text = client.upsert_one(row)
            total += 1
            count_in_prefix += 1

            if status >= 300:
                print(f"[ERR] {key} -> HTTP {status}: {text}")
            else:
                if total % 10 == 0:
                    print(f"Uploaded {total} images...")

            if SLEEP_SECONDS:
                time.sleep(SLEEP_SECONDS)

        print(f"Finished {prefix} ({count_in_prefix} objects)")

    print(f"\nDone. Total uploaded: {total}")

if __name__ == "__main__":
    main()
