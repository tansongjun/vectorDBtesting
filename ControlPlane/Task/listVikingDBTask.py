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
# List Tasks Function
# ────────────────────────────────────────────────
def list_vikingdb_tasks():
    print("\n=== List VikingDB Tasks (Control Plane) ===")
    print("Shows all your background tasks (import/export/update/delete).\n")

    project = input("Project name [AIAnimation]: ").strip() or "AIAnimation"
    collection = input("Collection name (optional, leave empty for all): ").strip()
    task_type = input("Task type (optional, e.g. data_export, data_import): ").strip()
    task_status = input("Task status (optional, e.g. success, failed, running): ").strip()
    page_str = input("Page number [1]: ").strip()
    page = int(page_str) if page_str.isdigit() else 1
    size_str = input("Page size [10]: ").strip()
    size = int(size_str) if size_str.isdigit() else 10

    body = {
        "ProjectName": project,
        "PageNumber": page,
        "PageSize": size
    }

    if collection:
        body["CollectionName"] = collection
    if task_type:
        body["TaskType"] = task_type
    if task_status:
        body["TaskStatus"] = task_status

    print(f"\nListing tasks (page {page}, size {size})...")
    print(f"Filters: project={project}, collection={collection or 'all'}, type={task_type or 'all'}, status={task_status or 'all'}")

    try:
        resp = call_control_plane("ListVikingdbTask", body)

        print("\nList Tasks Success!")
        print(json.dumps(resp, indent=2))

        result = resp.get("Result", {})
        tasks = result.get("Tasks", [])
        total = result.get("TotalCount", 0)

        print(f"\nFound {len(tasks)} tasks (total in project: {total})")
        if not tasks:
            print("No tasks found (or none match your filters).")
            return

        for task in tasks:
            print(f"\nTask ID: {task['TaskId']}")
            print(f"  Type: {task['TaskType']}")
            print(f"  Status: {task['TaskStatus']}")
            print(f"  Created: {task['CreateTime']}")
            print(f"  Updated: {task['UpdateTime']}")
            print(f"  Updated by: {task['UpdatePerson']}")
            print(f"  Collection: {task.get('TaskConfig', {}).get('CollectionName', 'N/A')}")
            progress = task.get("TaskProcessInfo", {}).get("TaskProgress", "N/A")
            print(f"  Progress: {progress}")
            error = task.get("TaskProcessInfo", {}).get("ErrorMessage", "")
            if error:
                print(f"  Error: {error}")
            print("-" * 60)

    except Exception as e:
        print("List tasks failed:", e)

# ────────────────────────────────────────────────
# Main Menu
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB Tool - List Tasks")
    print("===================================================\n")

    while True:
        print("\nOptions:")
        print("  1. List VikingDB tasks")
        print("  0. Exit")
        choice = input("Choose [1,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            list_vikingdb_tasks()
        else:
            print("Invalid choice. Try again.")