#!/usr/bin/env python3
"""
VikingDB Tool - Full Script with Keywords Search
- Control Plane: management APIs
- Data Plane: search (including keywords search)
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

# Control Plane (collections, indexes, etc.)
CP_HOST = "vikingdb.ap-southeast-1.byteplusapi.com"

# Data Plane (search, fetch, update, delete, keywords, etc.)
DP_HOST = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"

REGION  = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"


# ────────────────────────────────────────────────
# Shared Signing Helpers
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
# Generic API Caller (used by both planes)
# ────────────────────────────────────────────────
def call_vikingdb(
    action: str | None,
    body: dict,
    host: str,
    path: str = "/",
    version: str = VERSION,
    is_control_plane: bool = True
):
    method = "POST"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    query = {}
    if is_control_plane and action:
        query = {"Action": action, "Version": version}

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

    k_date    = hmac_sha256(SK.encode(), short_date)
    k_region  = hmac_sha256(k_date, REGION)
    k_service = hmac_sha256(k_region, SERVICE)
    k_signing = hmac_sha256(k_service, "request")
    signature = hmac_sha256(k_signing, string_to_sign).hex()

    headers = {
        "Content-Type": "application/json",
        "Host": host,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={AK}/{credential_scope}, "
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
# Keywords Search (Data Plane)
# ────────────────────────────────────────────────
def test_keywords_search():
    print("\n=== Test Keywords Search (text collection) ===")
    print("Requires a collection with text field + vectorization enabled.\n")

    collection = input("Text collection name: ").strip()
    if not collection:
        print("No collection name. Aborting.")
        return

    index_name = input("Index name: ").strip()
    if not index_name:
        print("No index name. Aborting.")
        return

    print("\nEnter 1–10 keywords (comma-separated)")
    print("Example: volcano,vector,search,billion")
    keywords_input = input("Keywords: ").strip()

    if not keywords_input:
        print("No keywords. Aborting.")
        return

    keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]
    if len(keywords) == 0:
        print("No valid keywords. Aborting.")
        return
    if len(keywords) > 10:
        print("Max 10 keywords — using first 10.")
        keywords = keywords[:10]

    limit_str = input("Limit / top K [5]: ").strip()
    limit = int(limit_str) if limit_str.isdigit() else 5

    case_str = input("Case sensitive? (y/n) [n]: ").strip().lower()
    case_sensitive = case_str == 'y'

    fields_input = input("Output fields (comma-separated, empty = all): ").strip()
    output_fields = [f.strip() for f in fields_input.split(",") if f.strip()] if fields_input else None

    body = {
        "collection_name": collection,
        "index_name": index_name,
        "keywords": keywords,
        "limit": limit,
        "case_sensitive": case_sensitive
    }
    if output_fields:
        body["output_fields"] = output_fields

    print(f"\nSearching: {keywords}")
    print(f"Collection: {collection} | Index: {index_name} | Limit: {limit}")

    try:
        resp = call_vikingdb(
            action=None,
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/keywords",
            is_control_plane=False
        )

        print("\nKeywords Search SUCCESS!")
        print(json.dumps(resp, indent=2))

        data = resp.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} results:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            print(f"    Score: {item.get('score'):.4f}")
            print(f"    ANN Score: {item.get('ann_score'):.4f}")
            fields = item.get("fields", {})
            if fields:
                print("    Fields:", json.dumps(fields, indent=2))
            print("-" * 60)

    except Exception as e:
        print("Keywords search failed:", e)
        if "404" in str(e):
            print("Try path variations:")
            print(" - /api/vikingdb/search/keywords")
            print(" - /api/vikingdb/data/keywords_search")
        elif "text" in str(e).lower() or "vectorize" in str(e).lower():
            print("Likely cause: Collection has no text field vectorization.")
            print("Solution: Check collection details or create a text-enabled collection.")
        return None


# ────────────────────────────────────────────────
# Main - Simple menu
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB Tool - Keywords Search Test")
    print("===================================\n")

    while True:
        print("\nOptions:")
        print("  1. Test keywords search (text collection)")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            test_keywords_search()

        else:
            print("Invalid choice. Try again.")