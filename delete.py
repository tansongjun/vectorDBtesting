#!/usr/bin/env python3
"""
VikingDB - Delete Data by ID or All (Data Plane)
Official endpoint: /api/vikingdb/data/delete
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
# Data Plane Caller
# ────────────────────────────────────────────────
def call_dataplane(body: dict, path: str):
    method = "POST"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

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
# Delete Data by ID or All
# ────────────────────────────────────────────────
def delete_data():
    print("\n=== Delete Data (Data Plane - DANGEROUS OPERATION) ===")
    print("WARNING: Deletion is asynchronous — data may still appear in search for ~5 minutes.")
    print("         Use with caution! No undo button.\n")

    collection = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
    delete_all = input("Delete ALL data in collection? (YES/NO) [NO]: ").strip().upper() == "YES"

    if delete_all:
        print("\n⚠️  You chose to delete ALL data! This cannot be undone.")
        confirm = input("Type 'DELETEALL' to confirm: ").strip()
        if confirm != "DELETEALL":
            print("Deletion cancelled.")
            return

        body = {
            "collection_name": collection,
            "del_all": True
        }

    else:
        print("\nExample IDs (from your previous multimodal search):")
        print("  2e717e2360f7ee23316966a8e482938a044329c6")
        print("  5fa00ced1ce876a90daae816de19ed93ede67995")
        id_input = input("\nEnter IDs to delete (comma-separated): ").strip()

        if not id_input:
            print("No IDs entered. Exiting.")
            return

        ids = [x.strip() for x in id_input.split(",") if x.strip()]
        if len(ids) > 100:
            print("Max 100 IDs per request. Truncating to first 100.")
            ids = ids[:100]

        body = {
            "collection_name": collection,
            "ids": ids
        }

    print(f"\nPreparing to delete from {collection}...")
    if delete_all:
        print("  → Deleting ALL data (del_all: true)")
    else:
        print(f"  → Deleting {len(ids)} specific ID(s)")

    confirm = input("\nType 'YES' to proceed with deletion: ").strip().upper()
    if confirm != "YES":
        print("Deletion cancelled.")
        return

    try:
        resp = call_dataplane(
            body=body,
            path="/api/vikingdb/data/delete"
        )

        print("\nDelete Request Success!")
        print(json.dumps(resp, indent=2))

        if resp.get("code") == "Success":
            print("\n✅ Deletion request sent successfully.")
            if delete_all:
                print("  All data deletion queued (may take ~5 min to fully reflect in index).")
            else:
                print(f"  {len(ids)} ID(s) queued for deletion.")
        else:
            print("\nAPI returned non-success code. Check response above.")

    except Exception as e:
        print("Delete failed:", e)

def update_data():
    print("\n=== Update Data (Partial Fields) ===")
    print("Supports updating scalar fields (e.g. created_at), text, or vector.")
    print("Vector update requires full 2048-dim vector — scalar is easier.\n")

    collection = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"

    print("\nExample ID (from previous multimodal search):")
    print("  2e717e2360f7ee23316966a8e482938a044329c6")
    id_input = input("\nEnter ID to update: ").strip()

    if not id_input:
        print("No ID entered. Aborting.")
        return

    print("\nCurrent timestamp example (you can change):")
    new_created_at = int(datetime.datetime.now().timestamp())
    print(f"  created_at → {new_created_at}")

    # Customize fields to update here
    fields_to_update = {
        "created_at": new_created_at,
        # Add more scalar/text fields if you have them, e.g.:
        # "status": "reviewed",
        # "confidence": 0.98,
        # "new_field": "some value"
    }

    # If you want to update vector (image), provide full 2048-dim vector:
    # fields_to_update["image"] = [0.1] * 2048   # ← example only

    print(f"\nWill update ID '{id_input}' with:")
    print(json.dumps(fields_to_update, indent=2))

    confirm = input("\nType 'UPDATE' to confirm: ").strip().upper()
    if confirm != "UPDATE":
        print("Update cancelled.")
        return

    body = {
        "collection_name": collection,
        "data": [
            {
                "id": id_input,
                **fields_to_update  # merge the fields to update
            }
        ]
    }

    print("\nSending update request...")

    try:
        resp = call_dataplane(
            body=body,
            path="/api/vikingdb/data/update"   # Official update path
        )

        print("\nUpdate Success!")
        print(json.dumps(resp, indent=2))

        if resp.get("code") == "Success":
            print("\n✅ Data updated successfully.")
            print("Note: Index may take a few seconds to reflect changes.")
            print("Verify with fetch_by_id or multimodal search.")
        else:
            print("API returned non-success code — check response.")

    except Exception as e:
        print("Update failed:", e)
# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB - Delete Data Tool (DANGEROUS!)")
    print("========================================\n")

    while True:
        print("\nOptions:")
        print("  1. Delete data (by ID or ALL)")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            delete_data()
        
        elif choice == "7":
            update_data()

        else:
            print("Invalid choice. Try again.")