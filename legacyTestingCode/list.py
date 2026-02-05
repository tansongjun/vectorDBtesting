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

def list_all_collections():
    page = 1
    page_size = 100
    collections = []

    while True:
        data = call_api(
            "ListVikingdbCollection",
            {
                "ProjectName": "AIAnimation",  # change if needed
                "PageNumber": page,
                "PageSize": page_size,
            },
        )

        result = data.get("Result", {})
        items = result.get("Collections", [])
        total = result.get("TotalCount", 0)

        collections.extend(items)

        if not items or len(collections) >= total:
            break

        page += 1

    return collections

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

def get_collection_details(
    collection_name: str = None,
    resource_id: str = None,
    project_name: str = "default"
):
    """
    Get detailed information about a VikingDB collection.
    Provide EITHER collection_name OR resource_id.
    """
    if not collection_name and not resource_id:
        raise ValueError("You must provide either collection_name or resource_id")

    body = {
        "ProjectName": project_name,
    }

    if collection_name:
        body["CollectionName"] = collection_name
    else:
        body["ResourceId"] = resource_id

    print(f"\nFetching details for collection...")
    if collection_name:
        print(f"→ CollectionName: {collection_name} (project: {project_name})")
    else:
        print(f"→ ResourceId: {resource_id} (project: {project_name})")
    print()

    try:
        result = call_api("GetVikingdbCollection", body)
    except RuntimeError as e:
        print(f"API call failed: {e}")
        return None

    print("\nCollection Details (full response):")
    print(json.dumps(result, indent=2))

    # Pretty-print key sections for easier reading
    res = result.get("Result", {})
    if not res:
        print("No 'Result' in response – check for errors above.")
        return result

    print("\nSummary:")
    print(f"  ProjectName:     {res.get('ProjectName')}")
    print(f"  ResourceId:      {res.get('ResourceId')}")
    print(f"  CollectionName:  {res.get('CollectionName')}")
    print(f"  Description:     {res.get('Description', '(empty)')}")
    print(f"  CreateTime:      {res.get('CreateTime')}")
    print(f"  UpdateTime:      {res.get('UpdateTime')}")
    print(f"  UpdatePerson:    {res.get('UpdatePerson')}")
    print(f"  EnableKeywordsSearch: {res.get('EnableKeywordsSearch')}")

    print("\nFields:")
    fields = res.get("Fields", [])
    if fields:
        for field in fields:
            print(f"  - {field.get('FieldName')}: type={field.get('FieldType')}", end="")
            if field.get("Dim"):
                print(f", dim={field.get('Dim')}", end="")
            if field.get("IsPrimaryKey"):
                print(" (PRIMARY KEY)", end="")
            if field.get("DefaultValue") is not None:
                print(f", default={field.get('DefaultValue')}", end="")
            print()
    else:
        print("  (no fields defined)")

    print("\nVectorization Config:")
    vec = res.get("Vectorize", {})
    if vec:
        dense = vec.get("Dense", {})
        if dense:
            print("  Dense:")
            print(f"    Model: {dense.get('ModelName')} v{dense.get('ModelVersion')}")
            print(f"    Field: text={dense.get('TextField')}, image={dense.get('ImageField')}")
            print(f"    Dim:   {dense.get('Dim') or '(model default)'}")
        sparse = vec.get("Sparse", {})
        if sparse:
            print("  Sparse:")
            print(f"    Model: {sparse.get('ModelName')} v{sparse.get('ModelVersion')}")
            print(f"    TextField: {sparse.get('TextField')}")
    else:
        print("  (no vectorization / auto-embedding configured)")

    stats = res.get("CollectionStats", {})
    print("\nStats:")
    print(f"  DataCount:    {stats.get('DataCount', 0)} entries")
    print(f"  DataStorage:  {stats.get('DataStorage', 0)} bytes")

    print("\nIndexes:")
    print(f"  Count: {res.get('IndexCount', 0)}")
    index_names = res.get("IndexNames", [])
    if index_names:
        print("  Names:")
        for idx in index_names:
            print(f"    - {idx}")
    else:
        print("  (no indexes yet)")

    return result

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
    
    # cols = list_all_collections()
    # print(f"\nFound {len(cols)} collections:\n")

    # for c in cols:
    #     print(
    #         f"- {c.get('CollectionName')} "
    #         f"| project={c.get('ProjectName')} "
    #         f"| resource_id={c.get('ResourceId')}"
    #     )
        
    get_collection_details(resource_id="vdb-acbe6daef04b42b6881f30dcada2ddde")
    # all_indexes = list_all_indexes()
    # print_indexes(all_indexes)
    get_index_details(
        index_name="idx_hnsw_1",           # ← change to your actual index name
        collection_name="ImageCollection"          # or "ImageCollection"
    )