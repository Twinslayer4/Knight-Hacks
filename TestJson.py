import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print(">>> Job started. Checking workspace files...")
file_path = "/Workspace/Users/boyishtomb519@gmail.com/Knight-Hacks/flipp_data.json"

if os.path.exists(file_path):
    print(f">>> Found flipp_data.json at {file_path}")
    
    # 1. Read JSON file directly with Spark to handle schema variations gracefully
    df = spark.read.option("multiline", "true").json(file_path)
    row_count = df.count()
    print(f">>> Successfully created Spark DataFrame with {row_count} rows.")
    
    if row_count > 0:
        # Register as a temporary view for Spark SQL
        df.createOrReplaceTempView("raw_flipp_data")
        
        # 2. Upsert parent table: Stores
        spark.sql("""
            MERGE INTO default.stores AS target
            USING (
                SELECT DISTINCT merchant_id AS store_id, merchant_name AS store_name 
                FROM raw_flipp_data 
                WHERE merchant_id IS NOT NULL
            ) AS source
            ON target.store_id = source.store_id
            WHEN NOT MATCHED THEN INSERT (store_id, store_name) 
            VALUES (source.store_id, source.store_name)
        """)
        print(">>> Stores table updated.")

        # 3. Upsert parent table: Items
        spark.sql("""
            MERGE INTO default.items AS target
            USING (
                SELECT DISTINCT id AS item_id, name AS item_name, category_l1 AS category 
                FROM raw_flipp_data 
                WHERE id IS NOT NULL
            ) AS source
            ON target.item_id = source.item_id
            WHEN NOT MATCHED THEN INSERT (item_id, item_name, category) 
            VALUES (source.item_id, source.item_name, source.category)
        """)
        print(">>> Items table updated.")

        # 4. Upsert associative table: Store-Product with regex BOGO check and explicit casting
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
        
    else:
        print(">>> Warning: The DataFrame is empty. No rows to process.")
else:
    print(f">>> CRITICAL: File not found at {file_path}")