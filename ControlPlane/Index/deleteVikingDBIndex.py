#!/usr/bin/env python3
"""
Test script: Delete a VikingDB Index (Control Plane V2)

WARNING: This is a DESTRUCTIVE operation!
- The specified index will be PERMANENTLY deleted.
- Data in the collection remains intact, but search/query performance
  may degrade if this was the only/primary index.
- There is NO UNDO.
- Use only on test indexes!

Usage:
    python delete_index_test.py

You will be prompted for confirmation twice.
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

# ────────────────────────────────────────────────
# Credentials & Config
# ────────────────────────────────────────────────
AK = os.getenv("AK")
SK = os.getenv("SK")

if not AK or not SK:
    raise ValueError("AK or SK missing in .env")

HOST = "vikingdb.ap-southeast-1.byteplusapi.com"      # Johor region
REGION = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"

PROJECT_NAME = "AIAnimation"   # ← change if your project is different


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
# Control Plane Caller
# ────────────────────────────────────────────────
def call_control_plane(action: str, body: dict):
    method = "POST"
    path = "/"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    query = {
        "Action": action,
        "Version": VERSION,
    }

    body_str = json.dumps(body, separators=(",", ":"))
    body_hash = sha256_hex(body_str)

    canonical_headers = (
        f"content-type:application/json\n"
        f"host:{HOST}\n"
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
        "Host": HOST,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    url = f"https://{HOST}{path}"
    response = requests.post(
        url,
        headers=headers,
        params=query,
        data=body_str,
        timeout=30,
    )

    if response.status_code != 200:
        print(f"HTTP {response.status_code}")
        print(response.text)
        raise RuntimeError("Control Plane API error")

    return response.json()


# ────────────────────────────────────────────────
# Main Test Function
# ────────────────────────────────────────────────
def delete_index_test():
    print("\n" + "="*60)
    print("!!! DANGER ZONE - DELETE INDEX TEST !!!")
    print("="*60)
    print("This will PERMANENTLY delete a VikingDB index.")
    print("The underlying data in the collection remains safe.")
    print("However, search performance may drop if this was the main index.\n")

    # 1. Collect required identifiers
    collection_name = input("Collection name (e.g. ImageCollection): ").strip()
    if not collection_name:
        print("No collection name provided. Aborting.")
        return

    index_name = input("Index name to DELETE (e.g. idx_hnsw_1): ").strip()
    if not index_name:
        print("No index name provided. Aborting.")
        return

    print(f"\nYou are about to DELETE index:")
    print(f"  Project:       {PROJECT_NAME}")
    print(f"  Collection:    {collection_name}")
    print(f"  Index:         {index_name}")
    print(f"  Region:        {REGION} (Johor)")
    print("\nThis action is IRREVERSIBLE!\n")

    # Safety check 1: echo the index name
    confirm1 = input("Type the exact index name to confirm: ").strip()
    if confirm1 != index_name:
        print("Confirmation mismatch. Aborting.")
        return

    # Safety check 2: magic phrase
    confirm2 = input("\nType 'DELETE INDEX FOREVER' to proceed: ").strip()
    if confirm2 != "DELETE INDEX FOREVER":
        print("Aborted.")
        return

    # Prepare request body
    body = {
        "ProjectName": PROJECT_NAME,
        "CollectionName": collection_name,
        "IndexName": index_name,
    }

    print("\nSending DeleteVikingdbIndex request...")
    print("Body:")
    print(json.dumps(body, indent=2))

    try:
        result = call_control_plane("DeleteVikingdbIndex", body)

        print("\nResponse:")
        print(json.dumps(result, indent=2))

        message = result.get("Result", {}).get("Message")
        if message == "success":
            print("\nSUCCESS: Index deleted!")
            req_id = result.get("ResponseMetadata", {}).get("RequestId")
            print(f"Request ID: {req_id}")
            print("You can verify by listing indexes or checking the console.")
        else:
            print("\nOperation completed but message != 'success' — check response above.")

    except Exception as e:
        print("\nFailed to delete index:")
        print(e)


if __name__ == "__main__":
    try:
        delete_index_test()
    except KeyboardInterrupt:
        print("\nAborted by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")