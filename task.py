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

def delete_vikingdb_task():
    print("\n=== Delete VikingDB Task (Remove Task Permanently) ===")
    print("This removes a task from your list — irreversible.\n")

    task_id = input("Task ID to delete (e.g. 3c8cddbd-7289-5a4a-8858-9505dcd49bfd): ").strip()
    if not task_id:
        print("No Task ID. Aborting.")
        return

    body = {
        "TaskId": task_id
    }

    print(f"\nDeleting task: {task_id}")

    confirm = input("\nType 'DELETE' to confirm (this is permanent): ").strip().upper()
    if confirm != "DELETE":
        print("Deletion cancelled.")
        return

    try:
        resp = call_control_plane("DeleteVikingdbTask", body)

        print("\nDelete Success!")
        print(json.dumps(resp, indent=2))

        print(f"\nTask {task_id} deleted.")
        print("Run 'List VikingDB tasks' (option 1) to confirm it's gone.")

    except Exception as e:
        print("Deletion failed:", e)
        
# ────────────────────────────────────────────────
# Safe Test: Create Filter Update Task (Updates ZERO records)
# ────────────────────────────────────────────────
def create_update_nothing_task():
    print("\n=== Safe Test: Create Filter Update Task (Updates ZERO records) ===")
    print("This creates a background task that matches NO real data.\n")

    collection = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"

    body = {
        "ProjectName": "AIAnimation",
        "CollectionName": collection,
        "TaskType": "filter_update",
        "TaskConfig": {
            "FilterConds": [
                {
                    "op": "range",
                    "field": "created_at",
                    "value": {
                        "gte": 1893456000,
                        "lt": 1893542400
                    }
                }
            ],
            "UpdateFields": {
                "created_at": 9999999999   # dummy value — won't apply
            },
            "NeedConfirm": False
        }
    }

    print("\nRequest preview (safe - updates nothing):")
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
            print("Task should finish quickly with success and 0 updates.")
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
        
def update_vikingdb_task():
    print("\n=== Test UpdateVikingdbTask (Confirm a 'confirm' task) ===")
    print("Only works on tasks in 'confirm' status — changes to 'confirmed'.\n")

    task_id = input("Task ID in 'confirm' status: ").strip()
    if not task_id:
        print("No Task ID. Aborting.")
        return

    body = {
        "TaskId": task_id
    }

    print(f"\nUpdating task {task_id} to 'confirmed'")

    confirm = input("\nType 'CONFIRM' to send update: ").strip().upper()
    if confirm != "CONFIRM":
        print("Update cancelled.")
        return

    try:
        resp = call_control_plane("UpdateVikingdbTask", body)

        print("\nUpdate Success!")
        print(json.dumps(resp, indent=2))

        print(f"\nTask {task_id} should now be 'confirmed' and starting.")
        print("Check status with 'List VikingDB tasks' (option 1) in 10 seconds.")

    except Exception as e:
        print("Update failed:", e)
# ────────────────────────────────────────────────
# Main Menu
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("VikingDB Tool - List & Safe Test Delete/Update Tasks")
    print("===================================================\n")

    while True:
        print("\nOptions:")
        print("  1. List VikingDB tasks")
        print("  2. Safe test: create filter_delete task (deletes NOTHING)")
        print("  3. Safe test: create filter_update task (updates NOTHING)")
        print("  0. Exit")
        choice = input("Choose [1,2,3,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "1":
            list_vikingdb_tasks()

        elif choice == "2":
            create_delete_nothing_task()

        elif choice == "3":
            get_vikingdb_task()

        elif choice == "4":
            delete_vikingdb_task()
        
        elif choice == "5":
            update_vikingdb_task()
        else:
            print("Invalid choice. Try again.")