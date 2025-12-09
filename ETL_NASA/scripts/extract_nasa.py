import json
import requests
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data/raw"
IMG = BASE / "data/images"
RAW.mkdir(parents=True, exist_ok=True)
IMG.mkdir(parents=True, exist_ok=True)

API_KEY = "LXpwZAiSTnAUYQ2VvJtkl7PBjhpdG9IV7sggKhrh"
URL = "https://api.nasa.gov/planetary/apod"

def extract_nasa_apod(count=1):
    r = requests.get(URL, params={"api_key": API_KEY, "count": count})
    r.raise_for_status()
    data = r.json()
    records = data if isinstance(data, list) else [data]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (RAW / f"nasa_apod_{ts}.json").write_text(json.dumps(records, indent=2))

    for i, d in enumerate(records, 1):
        if d.get("media_type") == "image":
            img_url = d.get("hdurl") or d.get("url")
            ext = Path(img_url).suffix or ".jpg"
            (IMG / f"nasa_apod_{d['date']}_{i}{ext}").write_bytes(
                requests.get(img_url).content
            )

    print("NASA APOD JSON + images extracted")

if __name__ == "__main__":
    extract_nasa_apod()