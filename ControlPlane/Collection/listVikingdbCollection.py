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

if __name__ == "__main__":
    
    cols = list_all_collections()
    print(f"\nFound {len(cols)} collections:\n")

    for c in cols:
        print(
            f"- {c.get('CollectionName')} "
            f"| project={c.get('ProjectName')} "
            f"| resource_id={c.get('ResourceId')}"
        )
    