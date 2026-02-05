#!/usr/bin/env python3
"""
Create a VikingDB collection (API V2 / Control Plane) in Asia Pacific (Johor).

Docs:
- Create Vikingdb Collection (Action=CreateVikingdbCollection)
- Control Plane API Calling Process (signature / endpoint / region)

Requirements:
  pip install requests python-dotenv
"""

import datetime
import hashlib
import hmac
import json
import os
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config (EDIT THESE)
# ----------------------------
AK = os.getenv("AK")
SK = os.getenv("SK")

# Asia Pacific (Johor) Control Plane endpoint + region
HOST = "vikingdb.ap-southeast-1.byteplusapi.com"
REGION = "ap-southeast-1"

# API V2 fixed version per docs
API_VERSION = "2025-06-09"

# Collection settings (EDIT THESE)
PROJECT_NAME = "AIAnimation"
COLLECTION_NAME = "ImageCollection"
DESCRIPTION = "collection created via CreateVikingdbCollection (Johor)"

VECTOR_DIM = 1024  # match your embedding dim


# ----------------------------
# Signing helpers (from Control Plane API Calling Process)
# ----------------------------
def norm_query(params: dict) -> str:
    query = ""
    for key in sorted(params.keys()):
        if isinstance(params[key], list):
            for v in params[key]:
                query += f"{quote(str(key), safe='-_.~')}={quote(str(v), safe='-_.~')}&"
        else:
            query += f"{quote(str(key), safe='-_.~')}={quote(str(params[key]), safe='-_.~')}&"
    query = query[:-1]
    return query.replace("+", "%20")


def hmac_sha256(key: bytes, content: str) -> bytes:
    return hmac.new(key, content.encode("utf-8"), hashlib.sha256).digest()


def hash_sha256_hex(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class VikingDBControlPlaneClient:
    def __init__(self, ak: str, sk: str, host: str, region: str, version: str = API_VERSION):
        self.ak = ak
        self.sk = sk
        self.host = host
        self.region = region
        self.service_code = "vikingdb"
        self.version = version

    def request(self, method: str, action: str, body: dict | None, query: dict | None = None, extra_headers: dict | None = None):
        if query is None:
            query = {}
        if extra_headers is None:
            extra_headers = {}

        request_body = "" if body is None else json.dumps(body)

        # Fixed path for control-plane is "/"
        path = "/"
        content_type = "application/json"
        now = datetime.datetime.utcnow()

        # Query must include Action + Version per docs
        full_query = {"Action": action, "Version": self.version, **query}

        x_date = now.strftime("%Y%m%dT%H%M%SZ")
        short_x_date = x_date[:8]
        x_content_sha256 = hash_sha256_hex(request_body)

        signed_headers_str = ";".join(["content-type", "host", "x-content-sha256", "x-date"])

        canonical_request = "\n".join([
            method.upper(),
            path,
            norm_query(full_query),
            "\n".join([
                f"content-type:{content_type}",
                f"host:{self.host}",
                f"x-content-sha256:{x_content_sha256}",
                f"x-date:{x_date}",
            ]),
            "",
            signed_headers_str,
            x_content_sha256,
        ])

        hashed_canonical_request = hash_sha256_hex(canonical_request)
        credential_scope = "/".join([short_x_date, self.region, self.service_code, "request"])
        string_to_sign = "\n".join(["HMAC-SHA256", x_date, credential_scope, hashed_canonical_request])

        k_date = hmac_sha256(self.sk.encode("utf-8"), short_x_date)
        k_region = hmac_sha256(k_date, self.region)
        k_service = hmac_sha256(k_region, self.service_code)
        k_signing = hmac_sha256(k_service, "request")
        signature = hmac_sha256(k_signing, string_to_sign).hex()

        authorization = f"HMAC-SHA256 Credential={self.ak}/{credential_scope}, SignedHeaders={signed_headers_str}, Signature={signature}"

        headers = {
            "Host": self.host,
            "Content-Type": content_type,
            "X-Date": x_date,
            "X-Content-Sha256": x_content_sha256,
            "Authorization": authorization,
            **extra_headers,
        }

        url = f"https://{self.host}{path}"
        resp = requests.request(method=method, url=url, headers=headers, params=full_query, data=request_body, timeout=30)

        # Try parse JSON; if not, return raw text
        try:
            return resp.status_code, resp.headers, resp.json()
        except Exception:
            return resp.status_code, resp.headers, {"raw_text": resp.text}


# ----------------------------
# Main: Create collection
# ----------------------------
def main():
    client = VikingDBControlPlaneClient(ak=AK, sk=SK, host=HOST, region=REGION)

    # ======================================
    # Create the COLLECTION (now active)
    # ======================================
    body = {
        "ProjectName": PROJECT_NAME,
        "CollectionName": COLLECTION_NAME,
        "Description": DESCRIPTION,
        "Fields": [
            {"FieldName": "id", "FieldType": "string", "IsPrimaryKey": True},
            {"FieldName": "image", "FieldType": "image"},
            {"FieldName": "created_at", "FieldType": "int64"},
        ],
        "Vectorize": {
            "Dense": {
                "ModelName": "skylark-embedding-vision",
                "ModelVersion": "250615",
                "ImageField": "image",
                "Dimension": VECTOR_DIM,
            }
        },
    }

    status, _, result = client.request(
        method="POST",
        action="CreateVikingdbCollection",
        body=body,
    )

    print("HTTP:", status)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()