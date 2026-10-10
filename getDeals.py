import requests
import re
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Initialize active Databricks Spark Engine
spark = SparkSession.builder.getOrCreate()

def fetch_raw_flipp_zip_payload():
    """
    Connects to the Parse.bot API using live parameters.
    Falls back gracefully to mock data if a 502/Gateway error occurs.
    """
    api_url = "https://api.parse.bot/scraper/054ce88f-2a08-4272-9ab9-70799328cedf/search_deals"
    headers = {"X-API-Key": "pmx_8e4f888f582c52ec6f8b69bd7a488af0"}
    params = {
        "query": "Buy one",
        "postal_code": "32816"
    } #change postal_code into user input. 
    
    try:
        print(">>> Contacting Parse.bot API...")
        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            payload = response.json()
            # Drill into Parse.bot's exact response path: data -> items
            items = payload.get("data", {}).get("items", [])
            print(f"Successfully retrieved {len(items)} live items from API.")
            return items
        else:
            print(f"API Warning: Received Status Code {response.status_code}. Activating mock fallback.")
    except Exception as e:
        print(f"API Connection Error: {e}. Activating mock fallback.")
    
    # --- GRACEFUL HACKATHON FALLBACK DATA ---
    # Field names have been updated here to match the exact keys returned by Parse.bot!
    return [
        {"merchant_name": "Walmart Neighborhood Market", "name": "Sparkling Water 12-Pack", "category_l2": "Beverages", "pre_price_text": "Buy 1 Get 1 Free", "current_price": 12.80},
        {"merchant_name": "Walmart Neighborhood Market", "name": "Thomas English Muffins", "category_l2": "Bakery", "pre_price_text": "BUY ONE GET ONE FREE!", "current_price": 5.50},
        {"merchant_name": "Walmart Neighborhood Market", "name": "Whole Milk Gal", "category_l2": "Dairy", "pre_price_text": "Save $1.00 on 1", "current_price": 6.00},
        {"merchant_name": "Target Supercenter", "name": "Dark Chocolate Bar 85%", "category_l2": "Snacks", "pre_price_text": "BOGO deals this week", "current_price": 3.00}
    ]

def ingest_flipp_bogo_pipeline():
    raw_items = fetch_raw_flipp_zip_payload()
    if not raw_items:
        print("No regional data available.")
        return

    
    flipp_schema = StructType([
        StructField("merchant_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category_l2", StringType(), True),
        StructField("pre_price_text", StringType(), True),
        StructField("current_price", DoubleType(), True)
    ])
    
    df_raw = spark.createDataFrame(raw_items, flipp_schema)
    df_raw.createOrReplaceTempView("vw_raw_flipp_feed")

    print(">>> Phase 1: Filtering the raw stream for BOGO keywords via Regex...")
    # FIXED: References newly adjusted schema columns (merchant_name, category_l2, pre_price_text)
    df_filtered_bogo = spark.sql("""
        SELECT 
            s.store_id,
            f.name AS item_name,
            f.category_l2 AS category,
            CAST(f.current_price AS DECIMAL(10,2)) AS price
        FROM vw_raw_flipp_feed f
        JOIN default.stores s ON LOWER(f.merchant_name) = LOWER(s.store_name)
        WHERE f.pre_price_text RLIKE '(?i)(BOGO|BUY\\\\s+1\\\\s+GET\\\\s+1|BUY\\\\s+ONE\\\\s+GET\\\\s+ONE)'
    """)
    df_filtered_bogo.createOrReplaceTempView("vw_filtered_bogo_deals")

    print(">>> Phase 2: Auditing and registering brand new unique items...")
    df_new_items = spark.sql("""
        SELECT 
            (SELECT COALESCE(MAX(item_id), 0) FROM default.items) + ROW_NUMBER() OVER (ORDER BY item_name) AS item_id,
            item_name,
            category
        FROM (
            SELECT DISTINCT item_name, category 
            FROM vw_filtered_bogo_deals
            WHERE LOWER(item_name) NOT IN (SELECT LOWER(item_name) FROM default.items)
        )
    """)
    
    new_item_count = df_new_items.count()
    if new_item_count > 0:
        df_new_items.write.mode("append").insertInto("default.items")
        print(f"Successfully registered {new_item_count} new product layout types into default.items.")
    
    print(">>> Phase 3: Performing atomic MERGE into default.store_product...")
    spark.sql("""
        MERGE INTO default.store_product AS target
        USING (
            SELECT 
                b.store_id, 
                i.item_id, 
                b.price
            FROM vw_filtered_bogo_deals b
            JOIN default.items i ON LOWER(b.item_name) = LOWER(i.item_name)
        ) AS source
        ON target.store_id = source.store_id AND target.item_id = source.item_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.bogo_promotion = true,
                target.price = source.price
        WHEN NOT MATCHED THEN
            INSERT (store_id, item_id, bogo_promotion, price) 
            VALUES (source.store_id, source.item_id, true, source.price)
    """)
    
    print(">>> Pipeline sync completed! Current state of default.store_product:")
    spark.table("default.store_product").show()

if __name__ == "__main__":
    ingest_flipp_bogo_pipeline()
