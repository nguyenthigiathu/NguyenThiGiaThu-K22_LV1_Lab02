import json
import psycopg2
import os
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="351999",
    port=5432
)

cursor = conn.cursor()

data_folder = "data"
total_inserted = 0

for file_name in os.listdir(data_folder):
    if not file_name.endswith(".json"):
        continue

    file_path = os.path.join(data_folder, file_name)
    print(f"Đang xử lý {file_name}...")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = []

        for item in data:
            if "id" not in item:
                continue

            records.append((
                item.get("id"),
                item.get("name"),
                item.get("price"),
                item.get("rating")
            ))

        if not records:
            continue

        insert_query = """
            INSERT INTO products (
                product_id,
                name,
                price,
                rating
            ) VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """

        execute_values(cursor, insert_query, records, page_size=1000)
        conn.commit()

        total_inserted += len(records)
        print(f"Inserted {len(records)} records")

    except Exception as e:
        print(f"Lỗi file {file_path}: {e}")
        conn.rollback()

cursor.close()
conn.close()

print(f"\nImport hoàn tất. Tổng records inserted: {total_inserted}")