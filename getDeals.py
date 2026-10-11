import os
import requests
import json
import pyspark.sql
from pyspark.sql import SparkSession

# Initialize active Databricks Spark Engine
spark = SparkSession.builder.getOrCreate()

def sanitize_items(items):
    """Normalize numeric types across items to prevent LongType vs DoubleType Spark inference conflicts."""
    if not items:
        return items
    
    # Check which keys contain floats, so we can harmonize ints to floats for those keys
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

def ingest_flipp_pipeline():
    api_url = "https://api.parse.bot/scraper/054ce88f-2a08-4272-9ab9-70799328cedf/search_deals"
    
    # Securely retrieve the API key from environment variables
    api_key = os.getenv("PARSE_BOT_API_KEY")
    if not api_key:
        raise ValueError(">>> Error: PARSE_BOT_API_KEY environment variable is not set.")

    headers = {"X-API-Key": api_key}
    
    # Hardcoded postal code (will be replaced by frontend input later)
    target_postal_code = "32816"
    
    params = {
        "query": "Buy one",
        "postal_code": target_postal_code
    }
    
    print(f">>> Contacting Parse.bot API for postal code: {target_postal_code} (waiting up to 60s for scraper)...")
    
    # Expanded timeout to 60 seconds to accommodate external scraper response times
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

    # Sanitize items to resolve integer/float type mismatches before Spark inference
    clean_items = sanitize_items(items)

    # Create Spark DataFrame directly from the sanitized items list in-memory
    df = spark.createDataFrame(clean_items)
    row_count = df.count()
    print(f">>> Successfully created Spark DataFrame with {row_count} rows.")

    if row_count > 0:
        # Register as temporary view
        df.createOrReplaceTempView("raw_flipp_data")
        
        # 1. Upsert parent table: Stores
        spark.sql("""
            MERGE INTO default.stores AS target
            USING (
                SELECT DISTINCT merchant_id AS store_id, merchant_name AS store_name 
                FROM raw_flipp_data 
                WHERE merchant_id IS NOT NULL AND merchant_name IS NOT NULL
            ) AS source
            ON target.store_id = source.store_id
            WHEN NOT MATCHED THEN INSERT (store_id, store_name) 
            VALUES (source.store_id, source.store_name)
        """)
        print(">>> Stores table updated.")

        # 2. Upsert parent table: Items
        spark.sql("""
            MERGE INTO default.items AS target
            USING (
                SELECT DISTINCT id AS item_id, name AS item_name, category_l1 AS category 
                FROM raw_flipp_data 
                WHERE id IS NOT NULL AND name IS NOT NULL
            ) AS source
            ON target.item_id = source.item_id
            WHEN NOT MATCHED THEN INSERT (item_id, item_name, category) 
            VALUES (source.item_id, source.item_name, source.category)
        """)
        print(">>> Items table updated.")

        # 3. Upsert associative table: Store-Product with strict sequential BOGO regex check
        spark.sql("""
            MERGE INTO default.store_product AS target
            USING (
                SELECT 
                    merchant_id AS store_id, 
                    id AS item_id, 
                    CASE 
                        WHEN lower(coalesce(sale_story, '')) RLIKE 'buy.*get.*free'
                             OR lower(coalesce(pre_price_text, '')) RLIKE 'buy.*get.*free'
                        THEN true 
                        ELSE false 
                    END AS bogo_promotion
                FROM raw_flipp_data
                WHERE merchant_id IS NOT NULL AND id IS NOT NULL
            ) AS source
            ON target.store_id = source.store_id AND target.item_id = source.item_id
            WHEN MATCHED THEN UPDATE SET 
                target.bogo_promotion = source.bogo_promotion
            WHEN NOT MATCHED THEN INSERT (store_id, item_id, bogo_promotion) 
                VALUES (source.store_id, source.item_id, source.bogo_promotion)
        """)
        print(">>> Store-Product associative table updated successfully!")
        
        # Show final result state
        spark.table("default.store_product").show()
    else:
        print(">>> Warning: DataFrame was empty. Skipping database merges.")

if __name__ == "__main__":
    ingest_flipp_pipeline()