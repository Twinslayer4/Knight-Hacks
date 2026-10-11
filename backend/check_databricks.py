from databricks import sql
import os

print("Connecting to Databricks...", flush=True)

with sql.connect(
    server_hostname="dbc-27a699b2-bbb7.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/5bc8676af1d56063",
    access_token=os.environ["DATABRICKS_TOKEN"]
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT s.store_name, s.address, i.item_name, sp.price
            FROM workspace.default.stores AS s
            JOIN workspace.default.store_product AS sp
                ON s.store_id = sp.store_id
            JOIN workspace.default.items AS i
                ON sp.item_id = i.item_id
            WHERE s.zip_code = ?
                AND sp.bogo_promotion = true
        """, ["90210"])

        rows = cursor.fetchall()

        for row in rows:
            print(row)

print("Connection worked!")