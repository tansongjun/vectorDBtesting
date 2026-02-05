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

        
# ────────────────────────────────────────────────
# Main – choose what to run
# ────────────────────────────────────────────────
def test_random_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # ← Use a valid index (from your list: idx_hnsw_1 or testing_only)
    limit: int = 5,
    output_fields: list[str] | None = None
):
    """
    Test Random Search – returns random records.
    Requires index_name (same pattern as other searches).
    """
    print(f"\n[Data Plane] Random Search → {collection_name} (index: {index_name}, {limit} items)")

    body = {
        "collection_name": collection_name,
        "index_name": index_name,          # ← This fixes the "missing index_name" error
        "limit": limit,
        # Optional: only return these fields
        # "output_fields": ["id", "image", "created_at"]
    }

    if output_fields:
        body["output_fields"] = output_fields

    try:
        result = call_vikingdb(
            action="", 
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/random",   # Confirmed from your doc
            version=VERSION
        )

        print("\nRandom Search SUCCESS!")
        print(json.dumps(result, indent=2))

        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} random items:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            fields = item.get("fields", {})
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            if "created_at" in fields:
                print(f"    Created at: {fields['created_at']}")
            print("-" * 60)

        return result

    except Exception as e:
        print(f"Random search failed: {e}")
        if "MissingParameter" in str(e) and "index_name" in str(e):
            print("Index name missing or invalid — use one from list_all_indexes (e.g. idx_hnsw_1)")
        elif "404" in str(e):
            print("Path issue — try '/api/vikingdb/data/random_search' or '/api/random/search'")
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
        print("  1. Test random search")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            lim = input("How many random items [5]: ").strip()
            limit = int(lim) if lim.isdigit() else 5
            test_random_search(collection_name=coll, limit=limit)

        else:
            print("Invalid choice.")