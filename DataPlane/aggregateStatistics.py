#!/usr/bin/env python3
"""
VikingDB - Fetch Data by ID (Data Plane)
Official endpoint: /api/vikingdb/data/fetch_in_collection
"""

import json
import hashlib
import hmac
import datetime
import os
from urllib.parse import quote
import requests

from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────
# Credentials & Config
# ────────────────────────────────────────────────
AK = os.getenv("AK")
SK = os.getenv("SK")

if not AK or not SK:
    raise ValueError("AK or SK missing in .env")

DP_HOST = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"
REGION  = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"


# ────────────────────────────────────────────────
# Signing Helpers
# ────────────────────────────────────────────────
def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def norm_query(params: dict) -> str:
    return "&".join(
        f"{quote(str(k), safe='-_.~')}={quote(str(v), safe='-_.~')}"
        for k, v in sorted(params.items())
    )


# ────────────────────────────────────────────────
# Data Plane Caller (for fetch/search)
# ────────────────────────────────────────────────
def call_dataplane(body: dict, path: str):
    method = "POST"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    # Data plane does NOT use Action/Version in query for fetch
    query = {}

    body_str = json.dumps(body, separators=(",", ":"))
    body_hash = sha256_hex(body_str)

    canonical_headers = (
        f"content-type:application/json\n"
        f"host:{DP_HOST}\n"
        f"x-content-sha256:{body_hash}\n"
        f"x-date:{x_date}\n"
    )

    signed_headers = "content-type;host;x-content-sha256;x-date"

    canonical_request = "\n".join([
        method,
        path,
        norm_query(query),
        canonical_headers,
        signed_headers,
        body_hash,
    ])

    credential_scope = f"{short_date}/{REGION}/{SERVICE}/request"
    string_to_sign = "\n".join([
        "HMAC-SHA256",
        x_date,
        credential_scope,
        sha256_hex(canonical_request),
    ])

    k_date    = hmac_sha256(SK.encode(), short_date)
    k_region  = hmac_sha256(k_date, REGION)
    k_service = hmac_sha256(k_region, SERVICE)
    k_signing = hmac_sha256(k_service, "request")
    signature = hmac_sha256(k_signing, string_to_sign).hex()

    headers = {
        "Content-Type": "application/json",
        "Host": DP_HOST,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    url = f"https://{DP_HOST}{path}"
    response = requests.post(
        url,
        headers=headers,
        params=query,
        data=body_str,
        timeout=30,
    )

    if response.status_code != 200:
        print(f"HTTP {response.status_code} | {path}")
        print(response.text)
        raise RuntimeError("Data Plane API error")

    return response.json()

def test_total_record_count():
    print("\n=== Test 1: Total Number of Records in ImageCollection ===")
    print("This uses Aggregate Statistics to count everything fast.\n")

    collection = "ImageCollection"
    index_name = input("Index name [idx_hnsw_1 or testing_only]: ").strip() or "idx_hnsw_1"

    body = {
        "collection_name": collection,
        "index_name": index_name,
        "op": "count"
    }

    print(f"\nCounting total records in '{collection}' using index '{index_name}'...")

    try:
        resp = call_dataplane(
            body=body,
            path="/api/vikingdb/data/agg"
        )

        print("\nSuccess!")
        print(json.dumps(resp, indent=2))

        total = resp.get("result", {}).get("agg", {}).get("__TOTAL__", "unknown")
        print(f"\nTotal number of records in {collection}: **{total}**")

        if total == "unknown":
            print("Couldn't find __TOTAL__ in response — check the full JSON above.")

    except Exception as e:
        print("Test failed:", e)
        print("\nCommon fixes:")
        print(" - Make sure index_name is correct (run list indexes to check)")
        print(" - Try the other index: testing_only or idx_hnsw_1")
# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB - Fetch by ID (Official Endpoint)")
    print("=========================================\n")

    while True:
        print("\nOptions:")
        print("  1. Fetch records by ID")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            test_total_record_count()
        else:
            print("Invalid choice. Try again.")