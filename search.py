#!/usr/bin/env python3
"""
BytePlus VikingDB Management + Search Script (Johor region)
- Control Plane: create/update/list collections & indexes
- Data Plane: vector search test

Run with: python this_script.py
"""

import json
import hashlib
import hmac
import datetime
import os
from urllib.parse import quote
import requests
import time

from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────
# Credentials & Config
# ────────────────────────────────────────────────
BYTEPLUS_VIKINGDB_AK = os.getenv("AK")
BYTEPLUS_VIKINGDB_SK = os.getenv("SK")

if not BYTEPLUS_VIKINGDB_AK or not BYTEPLUS_VIKINGDB_SK:
    raise ValueError("AK or SK missing! Check your .env file.")

# Control Plane (management)
CP_HOST = "vikingdb.ap-southeast-1.byteplusapi.com"

# Data Plane (search / upsert)
DP_HOST = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"

REGION = "ap-southeast-1"
SERVICE = "vikingdb"
VERSION = "2025-06-09"


# ────────────────────────────────────────────────
# Shared Helpers
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
# Generic API Caller (used by both Control & Data Plane)
# ────────────────────────────────────────────────
def call_vikingdb(
    action: str,
    body: dict,
    host: str,
    path: str = "/",
    version: str = VERSION
):
    method = "POST"

    now = datetime.datetime.now(datetime.timezone.utc)
    x_date = now.strftime("%Y%m%dT%H%M%SZ")
    short_date = x_date[:8]

    query = {
        "Action": action,
        "Version": version,
    }

    body_str = json.dumps(body, separators=(",", ":"))
    body_hash = sha256_hex(body_str)

    canonical_headers = (
        f"content-type:application/json\n"
        f"host:{host}\n"
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

    k_date = hmac_sha256(BYTEPLUS_VIKINGDB_SK.encode(), short_date)
    k_region = hmac_sha256(k_date, REGION)
    k_service = hmac_sha256(k_region, SERVICE)
    k_signing = hmac_sha256(k_service, "request")
    signature = hmac_sha256(k_signing, string_to_sign).hex()

    headers = {
        "Content-Type": "application/json",
        "Host": host,
        "X-Date": x_date,
        "X-Content-Sha256": body_hash,
        "Authorization": (
            f"HMAC-SHA256 Credential={BYTEPLUS_VIKINGDB_AK}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }

    url = f"https://{host}{path}"
    response = requests.post(
        url,
        headers=headers,
        params=query,
        data=body_str,
        timeout=30,
    )

    if response.status_code != 200:
        print(f"HTTP {response.status_code} | {host}{path}")
        print(response.text)
        raise RuntimeError("VikingDB API error")

    return response.json()
def test_id_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # Use your real index (e.g. idx_hnsw_1 or testing_only)
    ids_input: str = None
):
    """
    Test Id Search – exact lookup by primary key (SHA1 IDs).
    """
    print(f"\n[Data Plane] Id Search → {collection_name} (index: {index_name})")

    if not ids_input:
        print("\nExample IDs from previous multimodal search (copy-paste):")
        print("  2e717e2360f7ee23316966a8e482938a044329c6")
        print("  5fa00ced1ce876a90daae816de19ed93ede67995")
        ids_input = input("Enter ID(s) comma-separated: ").strip()
        if not ids_input:
            print("No IDs provided.")
            return None

    # Split and clean IDs
    ids_list = [id.strip() for id in ids_input.split(",") if id.strip()]

    # Body: use "id" as key with list (common pattern)
    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "id": ids_list,  # ← This is the key the API expects (not "ids")
        # Optional: return specific fields
        "output_fields": ["id", "image", "created_at"]
    }

    print("\nSending Id Search request...")
    print(f"  IDs: {ids_list}")

    try:
        result = call_vikingdb(
            action="", 
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/id",   # Confirmed from your pattern
            version=VERSION
        )

        print("\nId Search SUCCESS!")
        print(json.dumps(result, indent=2))

        # Parse results
        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} documents:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            fields = item.get("fields", {})
            if fields:
                print("    Fields:")
                for k, v in fields.items():
                    print(f"      {k}: {v}")
            print("-" * 60)

        return result

    except Exception as e:
        print(f"Id search failed: {e}")
        if "MissingParameter" in str(e) and "id" in str(e):
            print("API expects 'id' key — try sending as string if single ID:")
            print("  body['id'] = ids_list[0]  # instead of list")
        elif "404" in str(e):
            print("Path issue — try /api/vikingdb/data/id_search or /api/id/search")
        return None
        
def test_multimodal_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # ← Use your real index name!
    top_k: int = 5
):
    """
    Multimodal Search – official endpoint for your auto-vectorized image collection.
    Supports image-to-image, text-to-image, text+image, etc.
    """
    print(f"\n[Data Plane] Multimodal Search → {collection_name} (index: {index_name}, top {top_k})")

    print("\nSupported inputs:")
    print(" - Image only → image-to-image search")
    print(" - Text only → text-to-image search")
    print(" - Text + Image → fused search")

    use_text = input("Enter text prompt (or press Enter to skip): ").strip()
    use_image = input("Enter TOS image URL (tos://... or leave empty): ").strip()

    if not use_text and not use_image:
        print("You must provide at least text or image!")
        return None

    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "limit": top_k,
        # Optional: uncomment if you want instruction mode (adds real_text_query in response)
        # "need_instruction": True,
        # "output_fields": ["id", "image", "created_at"],
    }

    if use_text:
        body["text"] = use_text
    if use_image:
        body["image"] = use_image

    print("\nSending request...")
    print(f"  Text: {use_text or '(none)'}")
    print(f"  Image: {use_image or '(none)'}")

    try:
        # OFFICIAL PATH from your latest doc
        result = call_vikingdb(
            action="",                      # No Action needed
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/multi_modal",   # ← This is it!
            version=VERSION
        )

        print("\nMultimodal Search SUCCESS!")
        print(json.dumps(result, indent=2))

        # Parse results
        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} similar items:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            print(f"    Score: {item.get('score'):.4f}")
            print(f"    ANN Score: {item.get('ann_score'):.4f}")
            fields = item.get("fields", {})
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            print("-" * 60)

        if "real_text_query" in result.get("result", {}):
            print(f"\nReal text query used: {result['result']['real_text_query']}")

        return result

    except Exception as e:
        print(f"Search failed: {e}")
        return None

# ────────────────────────────────────────────────
# Data Plane – Vector Search
# ────────────────────────────────────────────────
def test_vector_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # ← CHANGE if your index name is different!
    top_k: int = 5,
    metric: str = "cosine",            # cosine | ip | l2 – match your index config
    output_fields: list[str] | None = None
):
    """
    Perform vector similarity search using official data-plane endpoint.
    Vector dim must be 2048 (from your Skylark-embedding-vision model).
    """
    print(f"\n[Data Plane] Vector Search → {collection_name} (index: {index_name}, top {top_k})")

    # Dummy query vector (all values 0.123) – length = 2048
    dummy_dim = 2048
    query_vector = [0.123] * dummy_dim
    print(f"  Using dummy query vector (length {len(query_vector)})")

    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "dense_vector": query_vector,
        "limit": top_k,
        "metric_type": metric,
        # Optional: uncomment to add
        # "filter": {"term": {"category": "truck"}},
        # "output_fields": ["id", "image", "created_at"],
    }

    if output_fields:
        body["output_fields"] = output_fields

    try:
        # Correct data-plane path + no Action in query
        result = call_vikingdb(
            action="",                      # ← No Action needed for this endpoint
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/vector",  # ← OFFICIAL PATH
            version=VERSION
        )

        print("\nSearch Response (Success!):")
        print(json.dumps(result, indent=2))

        # Parse results
        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} results:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            print(f"    Score: {item.get('score'):.4f}")
            print(f"    ANN Score: {item.get('ann_score'):.4f}")
            fields = item.get("fields", {})
            if fields:
                print("    Fields:", json.dumps(fields, indent=2))
            print("-" * 50)

        return result

    except Exception as e:
        print(f"Search failed: {e}")
        if "404" in str(e):
            print("\nStill 404? Try these minor path tweaks (change path=):")
            print(" - path='/api/vikingdb/data/v1/search/vector'")
            print(" - path='/api/vikingdb/search/vector'")
            print("\nAlso check:")
            print("1. Index name is correct (run list_all_indexes)")
            print("2. Collection has active vector index")
            print("3. AK/SK permissions for data-plane")
        return None
# ────────────────────────────────────────────────
# Main – choose what to run
# ────────────────────────────────────────────────
def test_random_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # ← Use a valid index (from your list: idx_hnsw_1 or testing_only)
    limit: int = 5,
    output_fields: list[str] | None = None
):
    """
    Test Random Search – returns random records.
    Requires index_name (same pattern as other searches).
    """
    print(f"\n[Data Plane] Random Search → {collection_name} (index: {index_name}, {limit} items)")

    body = {
        "collection_name": collection_name,
        "index_name": index_name,          # ← This fixes the "missing index_name" error
        "limit": limit,
        # Optional: only return these fields
        # "output_fields": ["id", "image", "created_at"]
    }

    if output_fields:
        body["output_fields"] = output_fields

    try:
        result = call_vikingdb(
            action="", 
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/random",   # Confirmed from your doc
            version=VERSION
        )

        print("\nRandom Search SUCCESS!")
        print(json.dumps(result, indent=2))

        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} random items:")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            fields = item.get("fields", {})
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            if "created_at" in fields:
                print(f"    Created at: {fields['created_at']}")
            print("-" * 60)

        return result

    except Exception as e:
        print(f"Random search failed: {e}")
        if "MissingParameter" in str(e) and "index_name" in str(e):
            print("Index name missing or invalid — use one from list_all_indexes (e.g. idx_hnsw_1)")
        elif "404" in str(e):
            print("Path issue — try '/api/vikingdb/data/random_search' or '/api/random/search'")
        return None

def test_scalar_search(
    collection_name: str = "ImageCollection",
    index_name: str = "idx_hnsw_1",    # Use a valid index name
    field: str = "created_at",         # Must be int64 or float32 with scalar index
    order: str = "desc",               # asc or desc
    limit: int = 5
):
    """
    Test Scalar Search – sort by scalar field (e.g. created_at desc).
    Endpoint: /api/vikingdb/data/search/scalar
    """
    print(f"\n[Data Plane] Scalar Search → {collection_name} (sort {field} {order}, top {limit})")

    body = {
        "collection_name": collection_name,
        "index_name": index_name,
        "field": field,
        "order": order,
        "limit": limit,
        "output_fields": ["id", "image", "created_at"]
    }

    try:
        result = call_vikingdb(
            action="", 
            body=body,
            host=DP_HOST,
            path="/api/vikingdb/data/search/scalar",   # Official path from docs
            version=VERSION
        )

        print("\nScalar Search SUCCESS!")
        print(json.dumps(result, indent=2))

        data = result.get("result", {}).get("data", [])
        print(f"\nFound {len(data)} items (sorted by {field} {order}):")
        for item in data:
            print(f"  • ID: {item.get('id')}")
            fields = item.get("fields", {})
            if field in fields:
                print(f"    {field}: {fields[field]}")
            if "image" in fields:
                print(f"    Image: {fields['image']}")
            print("-" * 60)

        return result

    except Exception as e:
        print(f"Scalar search failed: {e}")
        if "404" in str(e):
            print("Path variation — try '/api/vikingdb/data/scalar_search' or '/api/scalar/search'")
        if "InvalidParameter" in str(e):
            print("Check:")
            print("1. Field exists and is int64/float32 with scalar index")
            print("2. Index name is correct")
        return None    
if __name__ == "__main__":
    print("VikingDB Tool (Johor) - Ctrl+C to exit\n")
    # List indexes to get exact name
    indexes = call_vikingdb("ListVikingdbIndex", {
        "ProjectName": "AIAnimation",
        "CollectionName": "ImageCollection",
        "PageNumber": 1,
        "PageSize": 10
    }, CP_HOST)
    print("Indexes in ImageCollection:")
    print(json.dumps(indexes, indent=2))
    while True:
        print("\nOptions:")
        print("  1. Update collection description")
        print("  2. Update index description")
        print("  3. Test vector search (dummy vector)")
        print("  0. Exit")
        choice = input("Choose [1-3,0]: ").strip()

        if choice == "0":
            print("Goodbye.")
            break

        elif choice == "3":
            print("\nYour collection is MULTIMODAL → using image-to-image search")
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            k = input("Top K [5]: ").strip()
            top_k = int(k) if k.isdigit() else 5
            
            print("\nExample TOS URLs from your data:")
            print("tos://bucketforvectordbdemo/data/truck/001.jpg")
            print("tos://bucketforvectordbdemo/data/car/red_sports_car.jpg")
            
            test_multimodal_search(collection_name=coll, top_k=top_k)

        elif choice == "4":  # or change to 3 if you want to replace
            print("\nTesting Id Search (exact lookup by ID)")
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            test_id_search(collection_name=coll)
            
        elif choice == "5":
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            lim = input("How many random items [5]: ").strip()
            limit = int(lim) if lim.isdigit() else 5
            test_random_search(collection_name=coll, limit=limit)

        elif choice == "6":
            coll = input("Collection name [ImageCollection]: ").strip() or "ImageCollection"
            idx = input("Index name [idx_hnsw_1]: ").strip() or "idx_hnsw_1"
            fld = input("Scalar field to sort by [created_at]: ").strip() or "created_at"
            ord = input("Order [desc]: ").strip().lower() or "desc"
            lim = input("Limit [5]: ").strip()
            limit = int(lim) if lim.isdigit() else 5
            test_scalar_search(collection_name=coll, index_name=idx, field=fld, order=ord, limit=limit)
        else:
            print("Invalid choice.")