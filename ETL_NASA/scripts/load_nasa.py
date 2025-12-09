import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def load_nasa_apod_to_supabase():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "staged", "nasa_apod_transformed.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)

    # Ensure correct date type
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Batch size
    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]
        records = batch_df.where(pd.notnull(batch_df), None).to_dict("records")

        values = []

        for r in records:
            title = r["title"].replace("'", "''")
            explanation = r["explanation"].replace("'", "''")
            media_type = r["media_type"]
            image_url = r.get("hdurl") or r.get("url")

            media_type_sql = f"'{media_type}'" if media_type else "NULL"
            image_url_sql = f"'{image_url}'" if image_url else "NULL"

            values.append(
                f"('{r['date']}', '{title}', '{explanation}', "
                f"{media_type_sql}, {image_url_sql})"
            )

        insert_sql = (
            "INSERT INTO nasa_apod "
            "(date, title, explanation, media_type, image_url) "
            f"VALUES {','.join(values)};"
        )

        # Execute SQL via Supabase RPC
        supabase.rpc("execute_sql", {"query": insert_sql}).execute()

        print(f"Inserted rows {i + 1} → {min(i + batch_size, len(df))}")
        time.sleep(0.5)

    print("NASA APOD data successfully loaded")

if __name__ == "__main__":
    load_nasa_apod_to_supabase()