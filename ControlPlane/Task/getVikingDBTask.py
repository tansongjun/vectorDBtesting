#!/usr/bin/env python3
"""
VikingDB Tool - List Tasks & Safe Test Delete/Update Tasks (Control Plane)
- List background tasks
- Safe test: filter_delete task (deletes ZERO records)
- Safe test: filter_update task (updates ZERO records)
"""

import json
import hashlib
import hmac
import datetime
import os
import time
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

CP_HOST = "vikingdb.ap-southeast-1.byteplusapi.com"
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
        f"host:{CP_HOST}\n"
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
        "Host": CP_HOST,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    url = f"https://{CP_HOST}{path}"
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
        raise RuntimeError("Control Plane API error")

    return response.json()



def get_vikingdb_task():
    print("\n=== Test GetVikingdbTask (View Task Details) ===")
    print("Shows full info for one task by ID.\n")

    task_id = input("Task ID (e.g. 3c8cddbd-7289-5a4a-8858-9505dcd49bfd): ").strip()
    if not task_id:
        print("No Task ID. Aborting.")
        return

    body = {
        "TaskId": task_id
    }

    print(f"\nGetting details for Task ID: {task_id}")

    try:
        resp = call_control_plane("GetVikingdbTask", body)

        print("\nGet Task Success!")
        print(json.dumps(resp, indent=2))

        result = resp.get("Result", {})
        if result:
            print(f"\nTask Type: {result.get('TaskType', 'N/A')}")
            print(f"Status: {result.get('TaskStatus', 'N/A')}")
            print(f"Progress: {result.get('TaskProcessInfo', {}).get('TaskProgress', 'N/A')}")
            error = result.get('TaskProcessInfo', {}).get('ErrorMessage', '')
            if error:
                print(f"Error: {error}")
        else:
            print("No task details returned.")

    except Exception as e:
        print("Get task failed:", e)
        
# ────────────────────────────────────────────────
# Main Menu
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB Tool - Get Task Details")
    print("===================================================\n")

    while True:
        print("\nOptions:")
        print("  1. Get VikingDB task details")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            get_vikingdb_task()
        else:
            print("Invalid choice. Try again.")