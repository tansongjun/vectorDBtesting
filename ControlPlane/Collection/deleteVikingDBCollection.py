#!/usr/bin/env python3
"""
Test script: Delete a VikingDB Collection (Control Plane V2)

WARNING: This is a DESTRUCTIVE operation!
- The collection and ALL its data/indexes will be PERMANENTLY deleted.
- There is NO UNDO.
- Run only on test collections!

Usage:
    python delete_collection_test.py

You will be prompted to confirm twice.
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
# Signing Helpers (same as your other Control Plane scripts)
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
def delete_collection_test():
    print("\n" + "="*60)
    print("!!! DANGER ZONE - DELETE COLLECTION TEST !!!")
    print("="*60)
    print("This will PERMANENTLY delete a collection and ALL its data/indexes.")
    print("There is NO RECOVERY.\n")

    # 1. Ask for target (CollectionName or ResourceId)
    use_name = input("Use CollectionName (y) or ResourceId (n)? [y]: ").strip().lower() != "n"
    
    if use_name:
        collection_name = input("Collection name to DELETE: ").strip()
        if not collection_name:
            print("No name provided. Aborting.")
            return
        identifier = {"CollectionName": collection_name}
        display_id = f"CollectionName: {collection_name}"
    else:
        resource_id = input("ResourceId to DELETE: ").strip()
        if not resource_id:
            print("No ResourceId provided. Aborting.")
            return
        identifier = {"ResourceId": resource_id}
        display_id = f"ResourceId: {resource_id}"

    print(f"\nYou are about to DELETE:")
    print(f"  Project:   {PROJECT_NAME}")
    print(f"  {display_id}")
    print("  Region:    ap-southeast-1 (Johor)")
    print("\nThis action is IRREVERSIBLE!\n")

    # Safety check 1
    confirm1 = input("Type the collection name / resource id to confirm: ").strip()
    expected = collection_name if use_name else resource_id
    if confirm1 != expected:
        print("Confirmation mismatch. Aborting.")
        return

    # Safety check 2
    confirm2 = input("\nType 'DELETE FOREVER' to proceed: ").strip()
    if confirm2 != "DELETE FOREVER":
        print("Aborted.")
        return

    # Prepare request
    body = {
        "ProjectName": PROJECT_NAME,
        **identifier
    }

    print("\nSending DeleteVikingdbCollection request...")
    print("Body:")
    print(json.dumps(body, indent=2))

    try:
        result = call_control_plane("DeleteVikingdbCollection", body)
        
        print("\nResponse:")
        print(json.dumps(result, indent=2))

        message = result.get("Result", {}).get("Message")
        if message == "success":
            print("\nSUCCESS: Collection deleted!")
            req_id = result.get("ResponseMetadata", {}).get("RequestId")
            print(f"Request ID: {req_id}")
        else:
            print("\nOperation completed but message != 'success' — check response.")

    except Exception as e:
        print("\nFailed to delete collection:")
        print(e)


if __name__ == "__main__":
    try:
        delete_collection_test()
    except KeyboardInterrupt:
        print("\nAborted by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")