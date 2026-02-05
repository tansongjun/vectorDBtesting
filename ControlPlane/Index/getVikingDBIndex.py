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



def get_index_details(
    index_name: str,
    collection_name: str = None,
    resource_id: str = None,
    project_name: str = "default"
):
    """
    Get detailed information about a specific VikingDB index.
    Provide EITHER collection_name OR resource_id (for the parent collection).
    index_name is required.
    """
    if not index_name:
        raise ValueError("index_name is required")

    if not collection_name and not resource_id:
        raise ValueError("You must provide either collection_name or resource_id (for the parent collection)")

    body = {
        "ProjectName": project_name,
        "IndexName": index_name,
    }

    if collection_name:
        body["CollectionName"] = collection_name
    else:
        body["ResourceId"] = resource_id

    print("\nFetching index details...")
    print(f"→ IndexName: {index_name}")
    if collection_name:
        print(f"  From CollectionName: {collection_name} (project: {project_name})")
    else:
        print(f"  From ResourceId: {resource_id} (project: {project_name})")
    print()

    try:
        result = call_api("GetVikingdbIndex", body)
    except RuntimeError as e:
        print(f"API call failed: {e}")
        return None

    print("\nIndex Details (full response):")
    print(json.dumps(result, indent=2))

    # Pretty-print key sections for readability
    res = result.get("Result", {})
    if not res:
        print("No 'Result' in response – check for errors above.")
        return result

    print("\nSummary:")
    print(f"  IndexName:       {res.get('IndexName')}")
    print(f"  CollectionName:  {res.get('CollectionName')}")
    print(f"  ProjectName:     {res.get('ProjectName')}")
    print(f"  ResourceId:      {res.get('ResourceId')}")
    print(f"  Description:     {res.get('Description', '(empty)')}")

    print(f"  CpuQuota:        {res.get('CpuQuota')}")
    print(f"  ActualCU:        {res.get('ActualCU', 'N/A')}")
    print(f"  ShardPolicy:     {res.get('ShardPolicy')}")
    print(f"  ShardCount:      {res.get('ShardCount')}")

    vec = res.get("VectorIndex", {})
    if vec:
        print("\nVectorIndex Configuration:")
        print(f"    IndexType:   {vec.get('IndexType')}")
        print(f"    Distance:    {vec.get('Distance')}")
        print(f"    Quant:       {vec.get('Quant')}")
        if "HnswM" in vec:
            print(f"    HnswM:       {vec.get('HnswM')}")
            print(f"    HnswCef:     {vec.get('HnswCef')}")
            print(f"    HnswSef:     {vec.get('HnswSef')}")
        if "DiskannM" in vec:
            print(f"    DiskannM:    {vec.get('DiskannM')}")
            print(f"    DiskannCef:  {vec.get('DiskannCef')}")
            print(f"    PqCodeRatio: {vec.get('PqCodeRatio')}")
            print(f"    CacheRatio:  {vec.get('CacheRatio')}")

    scalars = res.get("ScalarIndex", [])
    print("\nScalarIndex fields:")
    if scalars:
        for s in scalars:
            if isinstance(s, dict):
                print(f"  - {s.get('FieldName')} (type: {s.get('FieldType')}, default: {s.get('DefaultValue', 'none')})")
            else:
                print(f"  - {s}")  # sometimes just string list
    else:
        print("  (none)")

    cost = res.get("IndexCost", {})
    if cost:
        print("\nIndexCost:")
        print(f"  CpuCore: {cost.get('CpuCore')}")
        print(f"  MemGb:   {cost.get('MemGb')}")

    return result
if __name__ == "__main__":
    get_index_details(
        index_name="idx_hnsw_1", # ← change to your actual index name
        collection_name="ImageCollection"
    )