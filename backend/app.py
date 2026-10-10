import os
from flask import Flask, jsonify, request
from databricks import sql

app = Flask(__name__)


def find_deals(zip_code):
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
            """, [zip_code])

            rows = cursor.fetchall()

            return [
                {
                    "store": row.store_name,
                    "address": row.address,
                    "item": row.item_name,
                    "price": str(row.price),
                    "offer": "Buy one, get one free",
                    "sample": True
                }
                for row in rows
            ]


@app.route("/")
def home():
    return "BOGO backend is working!"


@app.route("/deals")
def get_deals():
    zip_code = request.args.get("zip", "").strip()

    if len(zip_code) != 5 or not zip_code.isascii() or not zip_code.isdigit():
        return jsonify({"error": "Enter a five-digit ZIP code."}), 400

    deals = find_deals(zip_code)
    return jsonify({"zip": zip_code, "deals": deals})