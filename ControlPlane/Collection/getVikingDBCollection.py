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


if __name__ == "__main__":  
    get_collection_details(resource_id="vdb-acbe6daef04b42b6881f30dcada2ddde")
    