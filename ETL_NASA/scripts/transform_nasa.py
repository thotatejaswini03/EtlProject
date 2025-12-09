import pandas as pd
import os
import json
import glob

def transform_nasa_apod_data():
    
    os.makedirs("../data/staged", exist_ok=True)

    latest_file = sorted(glob.glob("../data/raw/nasa_apod_*.json"))[-1]

    with open(latest_file, "r") as f:
        data = json.load(f)

  
    if isinstance(data, dict):
        records = [data]
    else:
        records = data

   
    df = pd.DataFrame(records)

    #
    keep_cols = ["date", "title", "explanation", "media_type", "url", "hdurl"]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = None
    df = df[keep_cols]

    df["extracted_at"] = pd.Timestamp.now()

    df.to_csv("../data/staged/nasa_apod_transformed.csv", index=False)
    print("NASA APOD data transformed successfully")

if __name__ == "__main__":
    transform_nasa_apod_data()