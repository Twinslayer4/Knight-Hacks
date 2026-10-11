import os
import requests
from databricks import sql
from databricks.sdk.core import Config


def get_db_connection():
    cfg = Config()
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME") or cfg.host,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=lambda: cfg.authenticate
    )

def sanitize_items(items):
    """Normalize numeric types across items to prevent type mismatch issues."""
    if not items:
        return items
    
    float_keys = set()
    for item in items:
        for k, v in item.items():
            if isinstance(v, float):
                float_keys.add(k)
                
    sanitized = []
    for item in items:
        new_item = {}
        for k, v in item.items():
            if k in float_keys and isinstance(v, int):
                new_item[k] = float(v)
            else:
                new_item[k] = v
        sanitized.append(new_item)
    return sanitized

def ingest_flipp_pipeline(user_postal_code="32816"):
    api_url = "https://api.parse.bot/scraper/054ce88f-2a08-4272-9ab9-70799328cedf/search_deals"
    
    api_key = os.getenv("PARSE_BOT_API_KEY")
    if not api_key:
        raise ValueError(">>> Error: PARSE_BOT_API_KEY environment variable is not set.")

    headers = {"X-API-Key": api_key}
    params = {
        "query": "Buy one",
        "postal_code": user_postal_code if user_postal_code else "32816"
    }
    
    print(f">>> Contacting Parse.bot API for postal code: {params['postal_code']}...")
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

    print(">>> Sanitizing items...")
    clean_items = sanitize_items(items)
    print(f">>> Sanitization complete. Processing {len(clean_items)} items...")

    rows_to_insert = []
    bogo_count = 0

    for item in clean_items:
        merchant_id = str(item.get("merchant_id") or "")
        merchant_name = str(item.get("merchant_name") or "Unknown Store")
        item_id = str(item.get("id") or "")
        item_name = str(item.get("name") or "Unknown Item")
        category = str(item.get("category_l1") or "Uncategorized")
        
        # Restrict check strictly to sale_story (sales history)
        sale_story = str(item.get("sale_story", "") or "")
        
        raw_price = item.get("price")
        price = str(raw_price) if raw_price is not None else "N/A"

        if not merchant_id or not item_id:
            continue

        # Strict BOGO check: Must look at sale_story, contain "free" (or "bogo"), alongside buy/get
        text_to_check = sale_story.lower()
        bogo_promotion = (
            ("buy" in text_to_check and "get" in text_to_check and "free" in text_to_check) 
            or "bogo" in text_to_check
        )

        if bogo_promotion:
            bogo_count += 1

        rows_to_insert.append((merchant_id, merchant_name, item_id, item_name, category, price, bogo_promotion))

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            print(f">>> Executing bulk staging and MERGE for {len(rows_to_insert)} records...")
            
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(rows_to_insert))
            flat_params = [val for row in rows_to_insert for val in row]
            
            cursor.execute(f"""
                CREATE OR REPLACE TEMP VIEW staging_flipp_data AS 
                SELECT * FROM VALUES {placeholders} AS t(store_id, store_name, item_id, item_name, category, price, bogo_promotion)
            """, flat_params)

            cursor.execute("""
                MERGE INTO default.stores AS target
                USING (
                    SELECT DISTINCT store_id, store_name 
                    FROM staging_flipp_data 
                    WHERE store_id IS NOT NULL AND store_name IS NOT NULL
                ) AS source
                ON target.store_id = source.store_id
                WHEN NOT MATCHED THEN INSERT (store_id, store_name) 
                VALUES (source.store_id, source.store_name)
            """)

            cursor.execute("""
                MERGE INTO default.items AS target
                USING (
                    SELECT DISTINCT item_id, item_name, category 
                    FROM staging_flipp_data 
                    WHERE item_id IS NOT NULL AND item_name IS NOT NULL
                ) AS source
                ON target.item_id = source.item_id
                WHEN NOT MATCHED THEN INSERT (item_id, item_name, category) 
                VALUES (source.item_id, source.item_name, source.category)
            """)

            cursor.execute("""
                MERGE INTO default.store_product AS target
                USING (
                    SELECT store_id, item_id, price, bogo_promotion 
                    FROM staging_flipp_data 
                    WHERE store_id IS NOT NULL AND item_id IS NOT NULL
                ) AS source
                ON target.store_id = source.store_id AND target.item_id = source.item_id
                WHEN MATCHED THEN UPDATE SET 
                    target.bogo_promotion = source.bogo_promotion, 
                    target.price = source.price
                WHEN NOT MATCHED THEN INSERT (store_id, item_id, price, bogo_promotion) 
                    VALUES (source.store_id, source.item_id, source.price, source.bogo_promotion)
            """)

            print(f">>> Processing Complete: Ingested items. Found {bogo_count} true BOGO deals.")

    print(">>> Unity Catalog tables updated successfully via SQL Connector!")

if __name__ == "__main__":
    ingest_flipp_pipeline("32816")