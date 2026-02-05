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


# ────────────────────────────────────────────────
# Fetch by ID – Official Endpoint
# ────────────────────────────────────────────────
def fetch_by_id():
    print("\n=== Fetch Records by ID (Official Endpoint) ===")

    collection = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"

    print("\nExample IDs (from your previous multimodal search):")
    print("  2e717e2360f7ee23316966a8e482938a044329c6")
    print("  5fa00ced1ce876a90daae816de19ed93ede67995")
    id_input = input("\nEnter one or more IDs (comma-separated): ").strip()

    if not id_input:
        print("No IDs entered. Exiting.")
        return

    ids = [x.strip() for x in id_input.split(",") if x.strip()]

    body = {
        "collection_name": collection,
        "ids": ids,                     # Official key: "ids"
        "output_fields": ["id", "image", "created_at"]  # Optional
    }

    print(f"\nFetching {len(ids)} record(s)...")

    try:
        resp = call_dataplane(
            body=body,
            path="/api/vikingdb/data/fetch_in_collection"   # Official path
        )

        print("\nFetch Success!")
        print(json.dumps(resp, indent=2))

        fetched = resp.get("result", {}).get("fetch", [])
        not_exist = resp.get("result", {}).get("ids_not_exist", [])

        print(f"\nFound {len(fetched)} record(s):")
        for item in fetched:
            print(f"ID: {item['id']}")
            print("Fields:", json.dumps(item.get("fields", {}), indent=2))
            print("-" * 60)

        if not_exist:
            print(f"\nIDs not found: {', '.join(not_exist)}")

    except Exception as e:
        print("Fetch failed:", e)

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
            fetch_by_id()
        else:
            print("Invalid choice. Try again.")