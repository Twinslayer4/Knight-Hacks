import os
import requests
from databricks import sql

def ingest_flipp_pipeline(user_postal_code):
    api_url = "https://api.parse.bot/scraper/054ce88f-2a08-4272-9ab9-70799328cedf/search_deals"
    
    api_key = os.getenv("PARSE_BOT_API_KEY")
    if not api_key:
        raise ValueError(">>> Error: PARSE_BOT_API_KEY environment variable is not set.")

    headers = {"X-API-Key": api_key}
    params = {
        "query": "Buy one",
        "postal_code": user_postal_code
    }
    
    print(f">>> Contacting Parse.bot API for postal code: {user_postal_code}...")
    response = requests.get(api_url, headers=headers, params=params, timeout=60)
    
    if response.status_code == 200:
        payload = response.json()
        items = payload.get("data", {}).get("items", [])
        print(f">>> Successfully retrieved {len(items)} live items from API.")
    else:
        raise Exception(f"API Failed with Status Code {response.status_code}: {response.text}")

    if not items:
        print(">>> Warning: No items returned from API.")
        return

    # Connect to Databricks using the SQL connector instead of Spark
    with sql.connect(
        server_hostname="dbc-27a699b2-bbb7.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/5bc8676af1d56063",
        access_token=os.environ["DATABRICKS_TOKEN"]
    ) as connection:
        with connection.cursor() as cursor:
            for item in items:
                merchant_id = item.get("merchant_id")
                merchant_name = item.get("merchant_name")
                item_id = item.get("id")
                item_name = item.get("name")
                category = item.get("category_l1")
                sale_story = str(item.get("sale_story", "") or "")
                pre_price_text = str(item.get("pre_price_text", "") or "")
                price = item.get("price")

                if not merchant_id or not item_id:
                    continue

                # Determine BOGO promotion status using python string checks
                text_to_check = (sale_story + " " + pre_price_text).lower()
                bogo_promotion = "buy" in text_to_check and "get" in text_to_check and "free" in text_to_check

                # 1. Upsert Store
                cursor.execute("""
                    MERGE INTO workspace.default.stores AS target
                    USING (SELECT ? AS store_id, ? AS store_name) AS source
                    ON target.store_id = source.store_id
                    WHEN NOT MATCHED THEN INSERT (store_id, store_name) VALUES (source.store_id, source.store_name)
                """, [merchant_id, merchant_name])

                # 2. Upsert Item
                cursor.execute("""
                    MERGE INTO workspace.default.items AS target
                    USING (SELECT ? AS item_id, ? AS item_name, ? AS category) AS source
                    ON target.item_id = source.item_id
                    WHEN NOT MATCHED THEN INSERT (item_id, item_name, category) VALUES (source.item_id, source.item_name, source.category)
                """, [item_id, item_name, category])

                # 3. Upsert Store-Product relation
                cursor.execute("""
                    MERGE INTO workspace.default.store_product AS target
                    USING (SELECT ? AS store_id, ? AS item_id, ? AS price, ? AS bogo_promotion) AS source
                    ON target.store_id = source.store_id AND target.item_id = source.item_id
                    WHEN MATCHED THEN UPDATE SET target.bogo_promotion = source.bogo_promotion, target.price = source.price
                    WHEN NOT MATCHED THEN INSERT (store_id, item_id, price, bogo_promotion) VALUES (source.store_id, source.item_id, source.price, source.bogo_promotion)
                """, [merchant_id, item_id, price, bogo_promotion])

    print(">>> Unity Catalog tables updated successfully via SQL Connector!")

if __name__ == "__main__":
    ingest_flipp_pipeline("32816")