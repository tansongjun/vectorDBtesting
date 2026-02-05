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

def list_all_indexes(
    project_name: str = "AIAnimation",
    collection_names: list[str] | None = None,   # e.g. ["ImageCollection", "dataset"]
    status_filter: list[str] | None = None,      # e.g. ["READY"]
    index_name_keyword: str | None = None,       # partial name match
    page_size: int = 50
):
    """
    List all indexes in VikingDB (paginated), with optional filters.
    Returns a list of index details.
    """
    indexes = []
    page = 1

    filter_body = {}
    if collection_names:
        filter_body["CollectionName"] = collection_names
    if status_filter:
        filter_body["Status"] = status_filter
    if index_name_keyword:
        filter_body["IndexNameKeyword"] = index_name_keyword

    body_base = {
        "ProjectName": project_name,
        "PageSize": page_size,
    }
    if filter_body:
        body_base["Filter"] = filter_body

    print("\nListing indexes...")
    if collection_names:
        print(f"  Filtering by collections: {', '.join(collection_names)}")
    if status_filter:
        print(f"  Filtering by status: {', '.join(status_filter)}")
    if index_name_keyword:
        print(f"  Name keyword: '{index_name_keyword}'")
    print(f"  Project: {project_name}\n")

    while True:
        body = {**body_base, "PageNumber": page}

        try:
            data = call_api("ListVikingdbIndex", body)
        except RuntimeError as e:
            print(f"API error on page {page}: {e}")
            break

        result = data.get("Result", {})
        page_indexes = result.get("Indexes", [])
        total = result.get("TotalCount", 0)

        indexes.extend(page_indexes)

        print(f"Page {page}: fetched {len(page_indexes)} indexes (total so far: {len(indexes)} / {total})")

        if not page_indexes or len(indexes) >= total:
            break

        page += 1

    return indexes


def print_indexes(indexes_list):
    if not indexes_list:
        print("No indexes found.")
        return

    print(f"\nFound {len(indexes_list)} indexes:\n")
    for idx in indexes_list:
        print(f"Index: {idx.get('IndexName')}")
        print(f"  Collection:     {idx.get('CollectionName')}")
        print(f"  Project:        {idx.get('ProjectName')}")
        print(f"  ResourceId:     {idx.get('ResourceId')}")
        print(f"  Description:    {idx.get('Description', '(empty)')}")

        vec = idx.get("VectorIndex", {})
        if vec:
            print("  VectorIndex:")
            print(f"    Type:     {vec.get('IndexType')}")
            print(f"    Distance: {vec.get('Distance')}")
            print(f"    Quant:    {vec.get('Quant')}")
            if "HnswM" in vec:
                print(f"    HnswM:    {vec.get('HnswM')}")
                print(f"    HnswCef:  {vec.get('HnswCef')}")
                print(f"    HnswSef:  {vec.get('HnswSef')}")

        scalars = idx.get("ScalarIndex", [])
        if scalars:
            print("  ScalarIndex fields:")
            for s in scalars:
                print(f"    - {s.get('FieldName')} ({s.get('FieldType')})")

        print(f"  ShardPolicy:    {idx.get('ShardPolicy')}")
        print(f"  ShardCount:     {idx.get('ShardCount')}")
        print(f"  CpuQuota:       {idx.get('CpuQuota')}")
        print("-" * 60)

if __name__ == "__main__":
    
    all_indexes = list_all_indexes()
    print_indexes(all_indexes)
    