#!/usr/bin/env python3
"""
Fetch demographics and other breakdowns from Plausible Analytics API.

This script fetches breakdowns for country, device, browser, and source
for a given date range and saves the results in tab-separated format.

Usage:
    python3 fetch_demographics.py --start 2024-01-01 --end 2024-01-31
    python3 fetch_demographics.py --period 30d
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime
from pathlib import Path


def load_env():
    """Load environment variables from .env file if it exists."""
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


def get_config():
    """Get API configuration from environment variables."""
    load_env()

    base_url = os.environ.get("PLAUSIBLE_API_BASE_URL", "https://plausible.galaxyproject.eu")
    api_key = os.environ.get("PLAUSIBLE_API_KEY")
    site_id = os.environ.get("PLAUSIBLE_SITE_ID")

    if not api_key or api_key == "your-api-key-here":
        print("Error: PLAUSIBLE_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    if not site_id or site_id in ("your-site-domain-here", "example.com"):
        print("Error: PLAUSIBLE_SITE_ID not set.", file=sys.stderr)
        sys.exit(1)

    base_url = base_url.rstrip("/")
    return base_url, api_key, site_id


def fetch_breakdown(base_url, api_key, site_id, date_range, property_name, limit=1000):
    """
    Fetch breakdown for a specific property.
    """
    params = {
        "site_id": site_id,
        "property": property_name,
        "metrics": "visitors,pageviews,bounce_rate,visit_duration",
        "limit": str(min(limit, 1000)),
    }
    
    if isinstance(date_range, list):
        params["period"] = "custom"
        params["date"] = f"{date_range[0]},{date_range[1]}"
    else:
        params["period"] = date_range
    
    query_string = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = f"{base_url}/api/v1/stats/breakdown?{query_string}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    
    request = urllib.request.Request(url, headers=headers, method="GET")
    
    all_results = []
    page = 1
    
    while True:
        page_url = f"{url}&page={page}"
        request = urllib.request.Request(page_url, headers=headers, method="GET")
        
        try:
            with urllib.request.urlopen(request) as response:
                result = json.loads(response.read().decode("utf-8"))
                results = result.get("results", [])
                
                if not results:
                    break
                    
                all_results.extend(results)
                
                if len(results) < min(limit, 1000):
                    break
                    
                page += 1
                
        except urllib.error.HTTPError as e:
            print(f"Error fetching {property_name}: {e.code} {e.read().decode('utf-8')}", file=sys.stderr)
            return []
        except urllib.error.URLError as e:
            print(f"Connection error: {e.reason}", file=sys.stderr)
            sys.exit(1)
    
    return all_results


def format_bounce_rate(rate):
    if rate is None:
        return "-"
    return f"{int(round(rate))}%"


def format_duration(seconds):
    if not seconds:
        return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def save_results(results, property_name, output_path):
    """Save results to TSV."""
    # The key in the result depends on the property (e.g. "visit:source" -> "source")
    # But usually it's just the property name stripped of prefix or just the value
    # Plausible API v1 results usually look like: {"source": "Google", "visitors": 100, ...}
    
    # Determine the key for the dimension
    if not results:
        return
        
    # Get the first key that isn't a metric
    metrics = {"visitors", "pageviews", "bounce_rate", "visit_duration"}
    keys = list(results[0].keys())
    dimension_key = next((k for k in keys if k not in metrics), "name")
    
    with open(output_path, "w") as f:
        f.write(f"{dimension_key}\tVisitors\tPageviews\tBounce Rate\tDuration\n")
        for row in results:
            val = row.get(dimension_key, "Unknown")
            vis = row.get("visitors", 0)
            pv = row.get("pageviews", 0)
            br = format_bounce_rate(row.get("bounce_rate"))
            dur = format_duration(row.get("visit_duration"))
            f.write(f"{val}\t{vis}\t{pv}\t{br}\t{dur}\n")


def main():
    parser = argparse.ArgumentParser(description="Fetch demographics data")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--period", help="Time period (e.g. 30d, 6mo)")
    group.add_argument("--start", help="Start date YYYY-MM-DD")
    
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--output-dir", help="Directory to save files", default="data/fetched")
    
    args = parser.parse_args()
    
    if args.start and not args.end:
        parser.error("--start requires --end")
    
    base_url, api_key, site_id = get_config()
    
    if args.period:
        date_range = args.period
        date_str = f"{args.period}-{datetime.now().strftime('%d-%b-%Y').lower()}"
    else:
        date_range = [args.start, args.end]
        date_str = f"{args.start}-to-{args.end}"
    
    properties = {
        "visit:country": "countries",
        "visit:device": "devices",
        "visit:browser": "browsers",
        "visit:source": "sources"
    }
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Fetching demographics for {site_id} ({date_str})...")
    
    for prop, name in properties.items():
        print(f"  Fetching {name}...")
        results = fetch_breakdown(base_url, api_key, site_id, date_range, prop)
        
        filename = f"demographics-{name}-{date_str}.tab"
        output_path = output_dir / filename
        
        if results:
            save_results(results, prop, output_path)
            print(f"    Saved {len(results)} rows to {filename}")
        else:
            print(f"    No data found for {name}")

if __name__ == "__main__":
    main()
