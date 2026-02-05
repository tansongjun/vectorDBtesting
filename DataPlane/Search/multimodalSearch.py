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

        
def test_multimodal_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # ← Use your real index name!
    top_k: int = 5
):
    """
    Multimodal Search – official endpoint for your auto-vectorized image collection.
    Supports image-to-image, text-to-image, text+image, etc.
    """
    print(f"\n[Data Plane] Multimodal Search → {collection_name} (index: {index_name}, top {top_k})")

    print("\nSupported inputs:")
    print(" - Image only → image-to-image search")
    print(" - Text only → text-to-image search")
    print(" - Text + Image → fused search")

    use_text = input("Enter text prompt (or press Enter to skip): ").strip()
    use_image = input("Enter TOS image URL (tos://... or leave empty): ").strip()

    if not use_text and not use_image:
        print("You must provide at least text or image!")
        return None

    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "limit": top_k,
        # Optional: uncomment if you want instruction mode (adds real_text_query in response)
        # "need_instruction": True,
        # "output_fields": ["id", "image", "created_at"],
    }

    if use_text:
        body["text"] = use_text
    if use_image:
        body["image"] = use_image

    print("\nSending request...")
    print(f"  Text: {use_text or '(none)'}")
    print(f"  Image: {use_image or '(none)'}")

    try:
        # OFFICIAL PATH from your latest doc
        result = call_vikingdb(
            action="",                      # No Action needed
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/multi_modal",   # ← This is it!
            version=VERSION
        )

        print("\nMultimodal Search SUCCESS!")
        print(json.dumps(result, indent=2))

        # Parse results
        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} similar items:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            print(f"    Score: {item.get('score'):.4f}")
            print(f"    ANN Score: {item.get('ann_score'):.4f}")
            fields = item.get("fields", {})
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            print("-" * 60)

        if "real_text_query" in result.get("result", {}):
            print(f"\nReal text query used: {result['result']['real_text_query']}")

        return result

    except Exception as e:
        print(f"Search failed: {e}")
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
        print("  1. Test multimodal search")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            print("\nYour collection is MULTIMODAL → using image-to-image search")
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            k = input("Top K [5]: ").strip()
            top_k = int(k) if k.isdigit() else 5
            
            print("\nExample TOS URLs from your data:")
            print("tos://bucketforvectordbdemo/data/truck/001.jpg")
            print("tos://bucketforvectordbdemo/data/car/red_sports_car.jpg")
            
            test_multimodal_search(collection_name=coll, top_k=top_k)
       
        else:
            print("Invalid choice.")