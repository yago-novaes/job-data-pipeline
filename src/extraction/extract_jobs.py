import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def fetch_jsearch_data(query="Analytics Engineer in United States", num_pages=1):
    """
    Fetch job postings from JSearch API via RapidAPI.
    Targeting remote roles and USD salaries.
    """
    url = "https://jsearch.p.rapidapi.com/search"
    
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }

    querystring = {
        "query": query,
        "page": "1",
        "num_pages": str(num_pages),
        "date_posted": "all" # can change to 'today' or '3days' later
    }

    print(f"[{datetime.now()}] Requesting data for: {query}...")
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to JSearch: {e}")
        return None

def save_raw_json(data):
    """Save the JSON response to data/raw folder."""
    if not data:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/raw/jsearch_raw_{timestamp}.json"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"[{datetime.now()}] Success! File saved at: {output_path}")

if __name__ == "__main__":
    results = fetch_jsearch_data()
    save_raw_json(results)