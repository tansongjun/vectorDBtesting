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

# ────────────────────────────────────────────────
# Safe Test: Create Filter Delete Task (Deletes ZERO records)
# ────────────────────────────────────────────────
def create_delete_nothing_task():
    print("\n=== Safe Test: Create Filter Delete Task (Deletes ZERO records) ===")
    print("This creates a background task that matches NO real data.\n")

    collection = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"

    body = {
        "ProjectName": "AIAnimation",
        "CollectionName": collection,
        "TaskType": "filter_delete",
        "TaskConfig": {
            "FilterConds": [
                {
                    "op": "range",
                    "field": "created_at",
                    "value": {
                        "gte": 1893456000,   # 2030-01-01 — no data exists here
                        "lt": 1893542400     # 2030-01-02
                    }
                }
            ],
            "NeedConfirm": True
        }
    }

    print("\nRequest preview (safe - deletes nothing):")
    print(json.dumps(body, indent=2))

    confirm = input("\nType 'CREATE' to create this safe test task: ").strip().upper()
    if confirm != "CREATE":
        print("Task creation cancelled.")
        return

    try:
        resp = call_control_plane("CreateVikingdbTask", body)

        print("\nSafe Test Task Created!")
        print(json.dumps(resp, indent=2))

        task_id = resp.get("Result", {}).get("TaskId")
        if task_id:
            print(f"\nNew Task ID: {task_id}")
            print("Task should finish quickly with success and 0 deletions.")
            print("Checking status in 5 seconds...")

            time.sleep(5)

            list_body = {
                "ProjectName": "AIAnimation",
                "TaskId": task_id
            }
            list_resp = call_control_plane("ListVikingdbTask", list_body)
            print("\nLatest status:")
            print(json.dumps(list_resp, indent=2))

        else:
            print("No TaskId returned — check response above.")

    except Exception as e:
        print("Task creation failed:", e)

# ────────────────────────────────────────────────
# Main Menu
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB Tool - Create Tasks")
    print("===================================================\n")

    while True:
        print("\nOptions:")
        print("  1. Create VikingDB tasks")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            create_delete_nothing_task()

        else:
            print("Invalid choice. Try again.")