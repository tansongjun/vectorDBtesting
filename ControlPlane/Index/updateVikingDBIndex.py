import json
import hashlib
import hmac
import datetime
import os
from urllib.parse import quote
import requests


from dotenv import load_dotenv
import os

load_dotenv()
BYTEPLUS_VIKINGDB_AK = os.getenv("AK")
BYTEPLUS_VIKINGDB_SK = os.getenv("SK")

# ===============================
# 🌏 Johor (Asia Pacific)
# ===============================
HOST = "vikingdb.ap-southeast-1.byteplusapi.com"
REGION = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def norm_query(params: dict) -> str:
    return "&".join(
        f"{quote(str(k), safe='-_.~')}={quote(str(v), safe='-_.~')}"
        for k, v in sorted(params.items())
    )


def call_api(action: str, body: dict):
    method = "POST"
    uri = "/"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    # Action + Version go in QUERY
    query = {
        "Action": action,
        "Version": VERSION,
    }

    # ✅ BODY MUST BE VALID JSON
    body_str = json.dumps(body, separators=(",", ":"))
    body_hash = sha256_hex(body_str)

    canonical_headers = (
        "content-type:application/json\n"
        f"host:{HOST}\n"
        f"x-content-sha256:{body_hash}\n"
        f"x-date:{x_date}\n"
    )

    signed_headers = "content-type;host;x-content-sha256;x-date"

    canonical_request = "\n".join([
        method,
        uri,
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
        "Host": HOST,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={BYTEPLUS_VIKINGDB_AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    response = requests.post(
        f"https://{HOST}/",
        headers=headers,
        params=query,
        data=body_str,   # ✅ JSON BODY
        timeout=30,
    )

    if response.status_code != 200:
        print("HTTP", response.status_code)
        print(response.text)
        raise RuntimeError("BytePlus API error")

    return response.json()


def update_index(
    index_name: str,
    collection_name: str = None,
    resource_id: str = None,
    new_description: str | None = None,
    cpu_quota: int | None = None,
    shard_policy: str | None = None,      # "auto" or "custom"
    shard_count: int | None = None,
    scalar_index: list[str] | None = None,  # list of field names or [] for none
    project_name: str = "default"
):
    """
    Update a VikingDB index (e.g. description, CPU quota, sharding, scalar fields).
    Only send parameters you want to change.
    Provide EITHER collection_name OR resource_id.
    """
    if not index_name:
        raise ValueError("index_name is required")

    if not collection_name and not resource_id:
        raise ValueError("You must provide either collection_name or resource_id")

    body = {
        "ProjectName": project_name,
        "IndexName": index_name,
    }

    if collection_name:
        body["CollectionName"] = collection_name
    else:
        body["ResourceId"] = resource_id

    updated = False

    if new_description is not None:
        body["Description"] = new_description
        print(f"  → New description: {new_description}")
        updated = True

    if cpu_quota is not None:
        if not (1 <= cpu_quota <= 10240):
            raise ValueError("cpu_quota must be between 1 and 10240")
        body["CpuQuota"] = cpu_quota
        print(f"  → New CpuQuota: {cpu_quota}")
        updated = True

    if shard_policy is not None:
        if shard_policy not in ("auto", "custom"):
            raise ValueError("shard_policy must be 'auto' or 'custom'")
        body["ShardPolicy"] = shard_policy
        print(f"  → New ShardPolicy: {shard_policy}")
        updated = True

    if shard_count is not None:
        if not (1 <= shard_count <= 256):
            raise ValueError("shard_count must be between 1 and 256")
        body["ShardCount"] = shard_count
        print(f"  → New ShardCount: {shard_count}")
        updated = True

    if scalar_index is not None:
        body["ScalarIndex"] = scalar_index  # [] = no scalar index, None = keep current
        print(f"  → New ScalarIndex: {scalar_index}")
        updated = True

    if not updated:
        print("Nothing to update — no changes provided.")
        return None

    print("\nUpdate preview:")
    print(f"  Index: {index_name}")
    print(f"  Collection: {collection_name or resource_id} (project: {project_name})")
    print()

    confirm = input("Type 'YES' to send update request: ").strip().upper()
    if confirm != "YES":
        print("Update cancelled.")
        return None

    try:
        result = call_api("UpdateVikingdbIndex", body)
        print("\nUpdate response:")
        print(json.dumps(result, indent=2))

        message = result.get("Result", {}).get("Message")
        if message == "success":
            print("\n✅ Index updated successfully!")
        else:
            print("\nUpdate sent, but message is not 'success'")

        return result

    except RuntimeError as e:
        print(f"Update failed: {e}")
        return None
    
if __name__ == "__main__":
    
    update_index(
        index_name="idx_hnsw_1",           # ← your actual index name
        collection_name="ImageCollection",         # or use resource_id=...
        new_description="HNSW cosine index for image collection DB"
    )