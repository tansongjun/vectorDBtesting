import os
import requests
from volc_auth import prepare_request
import webbrowser
import json


AK = os.getenv("AK")
SK = os.getenv("SK")

DOMAIN = "api-vikingdb.vikingdb.ap-southeast-1.bytepluses.com"
PATH = "/api/vikingdb/data/search/multi_modal"

body = {
    "collection_name": "test",
    "index_name": "testindex",
    "text": "car",
    "need_instruction": True,
    "limit": 1,           # ← this controls how many results you get (up to what your index/collection allows)
    # "output_fields": ["image", "some_other_field"],  # optional but recommended
}

req = prepare_request(
    method="POST",
    path=PATH,
    ak=AK,
    sk=SK,
    host=DOMAIN,
    region="ap-southeast-1",
    service="vikingdb",
    data=body,
)

resp = requests.request(
    method=req.method,
    url=f"https://{DOMAIN}{req.path}",
    headers=req.headers,
    data=req.body,
    timeout=30,
)

print(resp.status_code)
print(resp.text)

if resp.status_code == 200:
    try:
        data = resp.json()["result"]["data"]
        print(f"Found {len(data)} results")
        
        for i, item in enumerate(data, 1):
            try:
                img_url = item["fields"]["image"]
                print(f"Result {i}: score = {item.get('score', '?.??')}, url = {img_url}")
                webbrowser.open(img_url)   # opens each in a new tab
            except (KeyError, TypeError):
                print(f"Result {i}: no image field found")
    except (KeyError, json.JSONDecodeError) as e:
        print("Could not parse results:", e)
else:
    print("Error:", resp.text)
