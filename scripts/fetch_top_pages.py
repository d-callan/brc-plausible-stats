#!/usr/bin/env python3
"""
Fetch top pages from Plausible Analytics API.

This script queries the Plausible Stats API v1 to retrieve top pages
for a given date range and saves the results in a tab-separated format
compatible with the analysis scripts.

Usage:
    python3 fetch_top_pages.py --start 2024-01-01 --end 2024-01-31
    python3 fetch_top_pages.py --period 30d
    python3 fetch_top_pages.py --period 6mo --output ../data/my-report.tab

Environment variables (set in .env file):
    PLAUSIBLE_API_BASE_URL - Base URL of your Plausible instance
    PLAUSIBLE_API_KEY      - Your Plausible Stats API key
    PLAUSIBLE_SITE_ID      - Your site domain as registered in Plausible
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
        print("Error: PLAUSIBLE_API_KEY not set or still has placeholder value.", file=sys.stderr)
        print("Please copy .env.example to .env and add your API key.", file=sys.stderr)
        sys.exit(1)

    if not site_id or site_id in ("your-site-domain-here", "example.com"):
        print("Error: PLAUSIBLE_SITE_ID not set or still has a placeholder value.", file=sys.stderr)
        print("Please copy .env.example to .env and set it to your site domain (e.g., brc-analytics.org).", file=sys.stderr)
        sys.exit(1)

    # Basic sanity check for base URL
    if not base_url.startswith("http://") and not base_url.startswith("https://"):
        print("Error: PLAUSIBLE_API_BASE_URL must start with http:// or https://", file=sys.stderr)
        sys.exit(1)

    # Strip trailing slash to avoid double slashes when building endpoint paths
    base_url = base_url.rstrip("/")

    return base_url, api_key, site_id


def fetch_top_pages(base_url, api_key, site_id, date_range, limit=1000):
    """
    Fetch top pages from Plausible API v1 breakdown endpoint.
    
    Args:
        base_url: Base URL of the Plausible instance (no trailing slash)
        api_key: Plausible Stats API key
        site_id: Site domain as registered in Plausible
        date_range: Either a preset string (e.g., "7d", "30d", "6mo") or
                    a list of two date strings ["YYYY-MM-DD", "YYYY-MM-DD"]
        limit: Maximum number of pages per request (max 1000 for v1 API)
    
    Returns:
        List of page data dictionaries
    """
    # Build query parameters for v1 API
    params = {
        "site_id": site_id,
        "property": "event:page",
        "metrics": "visitors,pageviews,bounce_rate,visit_duration",
        "limit": str(min(limit, 1000)),  # v1 API max is 1000
    }
    
    # Handle date range - v1 uses period + date params
    if isinstance(date_range, list):
        # Custom date range: ["YYYY-MM-DD", "YYYY-MM-DD"]
        params["period"] = "custom"
        params["date"] = f"{date_range[0]},{date_range[1]}"
    else:
        # Preset period like "7d", "30d", "6mo", etc.
        params["period"] = date_range
    
    # Build URL with query string
    query_string = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = f"{base_url}/api/v1/stats/breakdown?{query_string}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    
    request = urllib.request.Request(url, headers=headers, method="GET")
    
    all_results = []
    page = 1
    
    while True:
        # Add page parameter for pagination
        page_url = f"{url}&page={page}"
        request = urllib.request.Request(page_url, headers=headers, method="GET")
        
        try:
            with urllib.request.urlopen(request) as response:
                result = json.loads(response.read().decode("utf-8"))
                results = result.get("results", [])
                
                if not results:
                    break
                    
                all_results.extend(results)
                
                # If we got fewer than limit, we've reached the end
                if len(results) < min(limit, 1000):
                    break
                    
                page += 1
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            print(f"Error: API request failed with status {e.code}", file=sys.stderr)
            print(f"Response: {error_body}", file=sys.stderr)
            sys.exit(1)
        except urllib.error.URLError as e:
            print(f"Error: Could not connect to Plausible API: {e.reason}", file=sys.stderr)
            sys.exit(1)
    
    return all_results


def format_time_on_page(seconds):
    """Format seconds into human-readable time (e.g., '2m 30s')."""
    if seconds is None or seconds == 0:
        return "-"
    
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    else:
        return f"{secs}s"


def format_bounce_rate(rate):
    """Format bounce rate as percentage."""
    if rate is None:
        return "-"
    return f"{int(round(rate))}%"


def results_to_tsv(results):
    """
    Convert API results to tab-separated format.
    
    Args:
        results: List of result rows from Plausible API v1
                 Each row is a dict like: {"page": "/", "visitors": 100, ...}
    
    Returns:
        String in TSV format with header
    """
    lines = ["Page url\tVisitors\tPageviews\tBounce rate\tTime on Page"]
    
    for row in results:
        # v1 API returns flat objects with named keys
        page_url = row.get("page", "")
        visitors = row.get("visitors", 0)
        pageviews = row.get("pageviews", 0)
        bounce_rate = format_bounce_rate(row.get("bounce_rate"))
        time_on_page = format_time_on_page(row.get("visit_duration"))
        
        lines.append(f"{page_url}\t{visitors}\t{pageviews}\t{bounce_rate}\t{time_on_page}")
    
    return "\n".join(lines) + "\n"


def generate_output_filename(date_range):
    """Generate a default output filename based on date range."""
    today = datetime.now().strftime("%d-%b-%Y").lower()
    
    if isinstance(date_range, list):
        # Custom date range
        start, end = date_range
        return f"top-pages-{start}-to-{end}.tab"
    else:
        # Preset period
        return f"top-pages-{date_range}-{today}.tab"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch top pages from Plausible Analytics API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch last 30 days
    python3 fetch_top_pages.py --period 30d

    # Fetch specific date range
    python3 fetch_top_pages.py --start 2024-01-01 --end 2024-01-31

    # Fetch last 6 months with custom output file
    python3 fetch_top_pages.py --period 6mo --output ../data/report.tab

Available period presets:
    day, 7d, 28d, 30d, 91d, month, 6mo, 12mo, year, all
        """
    )
    
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--period",
        choices=["day", "7d", "28d", "30d", "91d", "month", "6mo", "12mo", "year", "all"],
        help="Preset time period to query"
    )
    date_group.add_argument(
        "--start",
        help="Start date (YYYY-MM-DD format). Must be used with --end"
    )
    
    parser.add_argument(
        "--end",
        help="End date (YYYY-MM-DD format). Must be used with --start"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: auto-generated in data/ directory)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of pages to fetch (default: 10000)"
    )
    
    args = parser.parse_args()
    
    # Validate date range arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")
    
    # Validate date format
    if args.start:
        try:
            datetime.strptime(args.start, "%Y-%m-%d")
            datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            parser.error("Dates must be in YYYY-MM-DD format")
    
    return args


def main():
    args = parse_args()
    
    # Get API configuration
    base_url, api_key, site_id = get_config()
    
    # Determine date range
    if args.period:
        date_range = args.period
    else:
        date_range = [args.start, args.end]
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        data_dir = Path(__file__).parent.parent / "data" / "fetched"
        output_path = data_dir / generate_output_filename(date_range)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Fetching top pages from Plausible at: {base_url}")
    print(f"Site: {site_id}")
    print(f"Date range: {date_range}")
    print(f"Limit: {args.limit} pages")
    
    # Fetch data
    results = fetch_top_pages(base_url, api_key, site_id, date_range, args.limit)
    
    if not results:
        print("Warning: No results returned from API", file=sys.stderr)
    else:
        print(f"Retrieved {len(results)} pages")
    
    # Convert to TSV and save
    tsv_content = results_to_tsv(results)
    
    with open(output_path, "w") as f:
        f.write(tsv_content)
    
    print(f"Saved to: {output_path}")
    print("\nYou can now run analysis with:")
    print(f"  python3 scripts/run_analysis.py {output_path}")


if __name__ == "__main__":
    main()
