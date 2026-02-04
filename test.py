import asyncio
import os
from tqdm import tqdm  # pip install tqdm
from volcengine.viking_db import VikingDBService, FieldType
from volcengine.viking_db.exception import VikingDBException   # ← NEW
from volcengine.viking_db import VikingDBService, FieldType, Field  # ← Add Field here
# ────────────────────────────────────────────────
# CONFIG - Update these!
# ────────────────────────────────────────────────
AK = os.getenv("AK")
SK = os.getenv("SK")
HOST = "api-vikingdb.mlp.ap-mya.byteplus.com"
REGION = "ap-southeast-1"

COLLECTION_NAME = "images_1000_test"
IMAGE_URL_PREFIX = "https://bucketforvectordbdemo.tos-ap-southeast-1.bytepluses.com/data/bird/"  # e.g. folder prefix

# Generate 1000 image URLs (adapt this to your actual files)
# Example: assuming files named img_0001.jpg to img_1000.jpg
IMAGE_URLS = [
    f"{IMAGE_URL_PREFIX}img_{i:04d}.jpg" for i in range(1, 1001)
]
# Or load from directory listing if local first:
# IMAGE_URLS = [f"{IMAGE_URL_PREFIX}{f}" for f in os.listdir("/local/path") if f.endswith(('.jpg', '.png'))]

CONCURRENCY_LIMIT = 10  # Start low; increase if no rate-limit errors

# ────────────────────────────────────────────────
vikingdb_service = VikingDBService(host=HOST, region=REGION, ak=AK, sk=SK)



async def create_or_get_collection():
    try:
        coll = await vikingdb_service.async_get_collection(COLLECTION_NAME)
        print(f"Collection '{COLLECTION_NAME}' already exists.")
        return coll
    except VikingDBException as e:
        if "not found" in str(e).lower() or e.code == 1000005:
            print(f"Creating collection '{COLLECTION_NAME}' with auto-vectorization support...")
            fields = [
                Field(
                    field_name="image_id",
                    field_type=FieldType.String,
                    is_primary_key=True
                ),
                Field(
                    field_name="image_url",
                    field_type=FieldType.String
                ),
                Field(
                    field_name="filename",
                    field_type=FieldType.String
                ),
            ]

            # Note: No 'vectorize' or 'primary_key' kwarg here — 
            # Auto-vectorization must be enabled in BytePlus console after creation,
            # or some SDK versions may not support it in create_collection.
            # If the call still fails, create via console first (recommended for now).

            coll = await vikingdb_service.async_create_collection(
                collection_name=COLLECTION_NAME,
                fields=fields,
                description="1000 images test with auto-vectorization (SDK minimal)"
            )
            print("Collection created successfully via SDK.")
            print("IMPORTANT: Go to BytePlus VikingDB console NOW → Edit this collection → Enable auto-vectorization on 'image_url' field → Select model (e.g. doubao-embedding-vision or bge-visualized-m3) → Save.")
            print("Also re-authorize TOS bucket access if prompted.")
            return coll
        else:
            raise e

async def upsert_one(coll, image_url, semaphore):
    async with semaphore:
        image_id = os.path.basename(image_url).split('.')[0]  # e.g. img_0001
        data = [{
            "image_id": image_id,
            "image_url": image_url,
            "filename": os.path.basename(image_url),
        }]
        try:
            await coll.async_upsert_data(data, async_upsert=True)  # async=True for higher throughput
            return True, image_id
        except Exception as e:
            return False, image_id, str(e)

async def bulk_upsert():
    coll = await create_or_get_collection()
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = []
    success_count = 0
    failed = []

    with tqdm(total=len(IMAGE_URLS), desc="Upserting images") as pbar:
        for url in IMAGE_URLS:
            task = asyncio.create_task(upsert_one(coll, url, semaphore))
            task.add_done_callback(lambda t: pbar.update(1))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                failed.append(("exception", str(res)))
            elif res[0]:
                success_count += 1
            else:
                failed.append((res[1], res[2]))

    print(f"\nSuccess: {success_count}/{len(IMAGE_URLS)}")
    if failed:
        print(f"Failed: {len(failed)}")
        for fid, err in failed[:10]:  # show first 10
            print(f"  - {fid}: {err}")
        print("... (truncated)")

    print("Ingestion complete. Wait ~minutes for data + hours for index to be searchable if async used.")
    print("Next: Create index via console/SDK, then test multimodal search.")

if __name__ == "__main__":
    asyncio.run(bulk_upsert())