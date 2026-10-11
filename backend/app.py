import os

from databricks import sql
from flask import Flask, jsonify, render_template, request
from getDeals import ingest_flipp_pipeline
from databricks.sdk.core import Config

app = Flask(__name__, template_folder='../templates')
print("ALL ENV VARS:", list(os.environ.keys()))
print("PARSE_BOT_API_KEY value:", os.environ.get("PARSE_BOT_API_KEY"))

def find_deals(zip_code):
    cfg = Config()
    with sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME") or cfg.host,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT s.store_name, i.item_name, sp.price, sp.bogo_promotion 
                FROM workspace.default.store_product sp
                JOIN workspace.default.stores s ON sp.store_id = s.store_id
                JOIN workspace.default.items i ON sp.item_id = i.item_id
                WHERE sp.bogo_promotion = TRUE
            """)
            rows = cursor.fetchall()
            
            deals_list = []
            for row in rows:
                deals_list.append({
                    "store": row[0],
                    "address": f"Zip Code: {zip_code}",
                    "item": row[1],
                    "price": row[2] if row[2] is not None else "N/A",
                    "offer": "Buy One Get One Free" if row[3] else "BOGO Deal"
                })
            return deals_list


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/api/search", methods=["POST"])
def search_deals():
    data = request.get_json() or {}
    zip_code = str(data.get("zip_code", "")).strip()

    if (
        len(zip_code) != 5
        or not zip_code.isascii()
        or not zip_code.isdigit()
    ):
        return jsonify({"error": "Enter a five-digit ZIP code."}), 400

    try:
        ingest_flipp_pipeline(zip_code)
        deals = find_deals(zip_code)
    except Exception:
        app.logger.exception("Failed to load deals from Databricks")
        return jsonify({
            "error": "Could not load deals. Please try again."
        }), 503

    return jsonify({
        "zip": zip_code, 
        "deals": deals
    })


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))
    app.run(host="0.0.0.0", port=port)