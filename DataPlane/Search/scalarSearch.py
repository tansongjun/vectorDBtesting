#!/usr/bin/env python3
"""
BytePlus VikingDB Management + Search Script (Johor region)
- Control Plane: create/update/list collections & indexes
- Data Plane: vector search test

Run with: python this_script.py
"""

import json
import hashlib
import hmac
import datetime
import os
from urllib.parse import quote
import requests
import time

from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────
# Credentials & Config
# ────────────────────────────────────────────────
BYTEPLUS_VIKINGDB_AK = os.getenv("AK")
BYTEPLUS_VIKINGDB_SK = os.getenv("SK")

if not BYTEPLUS_VIKINGDB_AK or not BYTEPLUS_VIKINGDB_SK:
    raise ValueError("AK or SK missing! Check your .env file.")

# Control Plane (management)
CP_HOST = "vikingdb.ap-southeast-1.byteplusapi.com"

# Data Plane (search / upsert)
DP_HOST = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"

REGION = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"


# ────────────────────────────────────────────────
# Shared Helpers
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
# Generic API Caller (used by both Control & Data Plane)
# ────────────────────────────────────────────────
def call_vikingdb(
    action: str,
    body: dict,
    host: str,
    path: str = "/",
    version: str = VERSION
):
    method = "POST"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    query = {
        "Action": action,
        "Version": version,
    }

    body_str = json.dumps(body, separators=(",", ":"))
    body_hash = sha256_hex(body_str)

    canonical_headers = (
        f"content-type:application/json\n"
        f"host:{host}\n"
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

    k_date = hmac_sha256(BYTEPLUS_VIKINGDB_SK.encode(), short_date)
    k_region = hmac_sha256(k_date, REGION)
    k_service = hmac_sha256(k_region, SERVICE)
    k_signing = hmac_sha256(k_service, "request")
    signature = hmac_sha256(k_signing, string_to_sign).hex()

    headers = {
        "Content-Type": "application/json",
        "Host": host,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={BYTEPLUS_VIKINGDB_AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    url = f"https://{host}{path}"
    response = requests.post(
        url,
        headers=headers,
        params=query,
        data=body_str,
        timeout=30,
    )

    if response.status_code != 200:
        print(f"HTTP {response.status_code} | {host}{path}")
        print(response.text)
        raise RuntimeError("VikingDB API error")

    return response.json()


def test_scalar_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # Use a valid index name
    field: str = "created_at",         # Must be int64 or float32 with scalar index
    order: str = "desc",               # asc or desc
    limit: int = 5
):
    """
    Test Scalar Search – sort by scalar field (e.g. created_at desc).
    Endpoint: /api/vikingdb/data/search/scalar
    """
    print(f"\n[Data Plane] Scalar Search → {collection_name} (sort {field} {order}, top {limit})")

    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "field": field,
        "order": order,
        "limit": limit,
        "output_fields": ["id", "image", "created_at"]
    }

    try:
        result = call_vikingdb(
            action="", 
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/scalar",   # Official path from docs
            version=VERSION
        )

        print("\nScalar Search SUCCESS!")
        print(json.dumps(result, indent=2))

        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} items (sorted by {field} {order}):")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            fields = item.get("fields", {})
            if field in fields:
                print(f"    {field}: {fields[field]}")
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            print("-" * 60)

        return result

    except Exception as e:
        print(f"Scalar search failed: {e}")
        if "404" in str(e):
            print("Path variation — try '/api/vikingdb/data/scalar_search' or '/api/scalar/search'")
        if "InvalidParameter" in str(e):
            print("Check:")
            print("1. Field exists and is int64/float32 with scalar index")
            print("2. Index name is correct")
        return None    
if __name__ == "__main__":
    print("VikingDB Tool (Johor) - Ctrl+C to exit\n")
    # List indexes to get exact name
    indexes = call_vikingdb("ListVikingdbIndex", {
        "ProjectName": "AIAnimation",
        "CollectionName": "ImageCollection",
        "PageNumber": 1,
        "PageSize": 10
    }, CP_HOST)
    print("Indexes in ImageCollection:")
    print(json.dumps(indexes, indent=2))
    while True:
        print("\nOptions:")
        print("  1. Test scalar search")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            idx = input("Index name [idx_hnsw_1]: ").strip() or "idx_hnsw_1"
            fld = input("Scalar field to sort by [created_at]: ").strip() or "created_at"
            ord = input("Order [desc]: ").strip().lower() or "desc"
            lim = input("Limit [5]: ").strip()
            limit = int(lim) if lim.isdigit() else 5
            test_scalar_search(collection_name=coll, index_name=idx, field=fld, order=ord, limit=limit)
        else:
            print("Invalid choice.")