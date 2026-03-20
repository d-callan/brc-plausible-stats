#!/usr/bin/env python3
"""
Fetch workflow landing request data from Grafana/InfluxDB.

This script queries the Galaxy stats Grafana instance to retrieve workflow
landing requests that originated from BRC Analytics. Data is saved in JSON
format for processing by report generation scripts.

Usage:
    python3 fetch_grafana_landings.py
    python3 fetch_grafana_landings.py --start-month 2024-10 --end-month 2025-12

Environment variables (set in .env file):
    GRAFANA_API_URL  - Base URL of the Grafana instance
    GRAFANA_API_KEY  - Grafana API key with read access
"""

import argparse
import calendar
import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime
from pathlib import Path

# Import shared taxonomy module for community classification
from taxonomy_cache import load_cache, get_community

# Workflow category patterns (same as in generate_monthly_summary_html.py)
WORKFLOW_CATEGORIES = {
    'Variant Calling': ['variant-calling', 'haploid-variant'],
    'Transcription': ['rnaseq', 'lncRNAs', 'transcriptome'],
    'Single Cell': ['scrna-seq', '10x-', 'cellplex', 'single-cell'],
    'Epigenomics': ['chipseq', 'atacseq', 'cutandrun', 'consensus-peaks'],
    'AMR': ['amr-gene', 'antimicrobial'],
    'Viral': ['viral', 'sars-cov', 'covid'],
}


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
    """Get Grafana API configuration from environment variables."""
    load_env()

    api_url = os.environ.get("GRAFANA_API_URL", "https://stats.galaxyproject.org")
    api_key = os.environ.get("GRAFANA_API_KEY")

    if not api_key or api_key == "your-grafana-api-key-here":
        print("Error: GRAFANA_API_KEY not set or still has placeholder value.", file=sys.stderr)
        print("Please add your Grafana API key to .env file.", file=sys.stderr)
        sys.exit(1)

    # Strip trailing slash
    api_url = api_url.rstrip("/")

    return api_url, api_key


def classify_workflow_category(workflow_name):
    """Classify a workflow into a category based on its name."""
    if not workflow_name:
        return 'Other'
    
    workflow_lower = workflow_name.lower()
    
    for category, patterns in WORKFLOW_CATEGORIES.items():
        for pattern in patterns:
            if pattern.lower() in workflow_lower:
                return category
    
    return 'Other'


def fetch_landing_data(api_url, api_key):
    """
    Fetch workflow landing request data from Grafana/InfluxDB.
    
    Fetches all available data (24 month window) in one query.
    
    Args:
        api_url: Base URL of the Grafana instance
        api_key: Grafana API key
    
    Returns:
        Raw InfluxDB response dict or None on error
    """
    # Query the monthly workflow/origin/dbkey measurement for BRC origin
    # Using the main_sql database which contains Galaxy Main metrics
    # This gets all data in the 24-month retention window
    query = """
        SELECT last("count") AS "count" 
        FROM "workflow_landing_requests_monthly_workflow_origin_dbkey" 
        WHERE "origin" = 'https://brc-analytics.org/' 
        GROUP BY "dbkey"::tag, "month"::tag, "origin"::tag, "workflow_name"::tag
    """
    
    # URL encode the query
    params = {
        "db": "main_sql",
        "q": query
    }
    
    url = f"{api_url}/api/datasources/proxy/3/query"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    data = urllib.parse.urlencode(params).encode('utf-8')
    request = urllib.request.Request(url, data=data, headers=headers, method="POST")
    
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"Error: Grafana API request failed with status {e.code}", file=sys.stderr)
        print(f"Response: {error_body}", file=sys.stderr)
        return None
    except urllib.error.URLError as e:
        print(f"Error: Could not connect to Grafana API: {e.reason}", file=sys.stderr)
        return None


def parse_influx_response(response):
    """
    Parse InfluxDB response into structured data.
    
    Args:
        response: Raw response from InfluxDB query
    
    Returns:
        List of dicts with month, workflow_name, dbkey, count
    """
    results = []
    
    if not response or "results" not in response:
        return results
    
    for result in response.get("results", []):
        for series in result.get("series", []):
            tags = series.get("tags", {})
            workflow_name = tags.get("workflow_name", "unknown")
            dbkey = tags.get("dbkey", "unknown")
            month = tags.get("month", "unknown")  # Month tag from GROUP BY
            
            columns = series.get("columns", [])
            values = series.get("values", [])
            
            # Find count column index
            count_idx = columns.index("count") if "count" in columns else 1
            
            for row in values:
                count = row[count_idx] if count_idx < len(row) else 0
                
                if count and count > 0:
                    results.append({
                        "month": month,
                        "workflow_name": workflow_name,
                        "dbkey": dbkey,
                        "count": int(count) if count else 0
                    })
    
    return results


def aggregate_by_month(data_points, taxonomy_cache, assembly_cache):
    """
    Aggregate data points by month with community and category classification.
    
    Args:
        data_points: List of parsed data points
        taxonomy_cache: Taxonomy cache for organism lookups
        assembly_cache: Assembly cache for dbkey -> taxid mapping
    
    Returns:
        Dict keyed by month (YYYY-MM) with aggregated stats
    """
    monthly_data = {}
    
    for point in data_points:
        # Get month from tag (format: "YYYY-MM-DD" or "YYYY-MM")
        month_tag = point.get("month", "unknown")
        if month_tag == "unknown":
            continue
        
        # Extract YYYY-MM from the month tag (may be YYYY-MM-DD or YYYY-MM)
        try:
            if len(month_tag) >= 7:
                month_key = month_tag[:7]  # Get YYYY-MM part
            else:
                continue
        except (ValueError, AttributeError):
            continue
        
        if month_key not in monthly_data:
            monthly_data[month_key] = {
                "total_landings": 0,
                "by_workflow": {},
                "by_category": {},
                "by_community": {},
                "by_dbkey": {},
                "raw_points": []
            }
        
        workflow_name = point["workflow_name"]
        dbkey = point["dbkey"]
        count = point["count"]
        
        # Classify workflow category
        category = classify_workflow_category(workflow_name)
        
        # Classify community from dbkey (assembly ID)
        community = "Other"
        if dbkey and dbkey != "unknown":
            # Try to get taxid from assembly cache
            # Try both formats: GCA_000193105.1 and GCA_000193105_1
            assembly_info = assembly_cache.get(dbkey)
            if not assembly_info:
                # Try with underscore instead of dot for version
                normalized_key = dbkey.replace('.', '_')
                assembly_info = assembly_cache.get(normalized_key)
            
            if assembly_info:
                # Handle both dict format and direct taxid
                if isinstance(assembly_info, dict):
                    taxid = assembly_info.get("taxid") or assembly_info.get("tax_id")
                    lineage = assembly_info.get("lineage", "")
                else:
                    # assembly_info might be the taxid directly
                    taxid = assembly_info
                    lineage = ""
                
                # If we have taxid but no lineage, look it up in taxonomy cache
                if taxid and not lineage:
                    tax_entry = taxonomy_cache.get(str(taxid))
                    if tax_entry and isinstance(tax_entry, dict):
                        lineage = tax_entry.get("lineage", "")
                
                if lineage:
                    community = get_community(lineage)
        
        # Aggregate
        monthly_data[month_key]["total_landings"] += count
        
        # By workflow
        if workflow_name not in monthly_data[month_key]["by_workflow"]:
            monthly_data[month_key]["by_workflow"][workflow_name] = 0
        monthly_data[month_key]["by_workflow"][workflow_name] += count
        
        # By category
        if category not in monthly_data[month_key]["by_category"]:
            monthly_data[month_key]["by_category"][category] = 0
        monthly_data[month_key]["by_category"][category] += count
        
        # By community
        if community not in monthly_data[month_key]["by_community"]:
            monthly_data[month_key]["by_community"][community] = 0
        monthly_data[month_key]["by_community"][community] += count
        
        # By dbkey
        if dbkey not in monthly_data[month_key]["by_dbkey"]:
            monthly_data[month_key]["by_dbkey"][dbkey] = 0
        monthly_data[month_key]["by_dbkey"][dbkey] += count
        
        # Keep raw point for detailed reports
        monthly_data[month_key]["raw_points"].append(point)
    
    return monthly_data


def get_month_range(year, month):
    """Get the first and last day of a month as YYYY-MM-DD strings."""
    first_day = f"{year:04d}-{month:02d}-01"
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = f"{year:04d}-{month:02d}-{last_day_num:02d}"
    return first_day, last_day


def parse_month(month_str):
    """Parse a YYYY-MM string into (year, month) tuple."""
    parts = month_str.split("-")
    return int(parts[0]), int(parts[1])


def month_iterator(start_year, start_month, end_year, end_month):
    """Iterate over months from start to end (inclusive)."""
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def main():
    parser = argparse.ArgumentParser(
        description="Fetch workflow landing data from Grafana"
    )
    parser.add_argument(
        "--start-month",
        default="2024-10",
        help="Start month in YYYY-MM format (default: 2024-10)"
    )
    parser.add_argument(
        "--end-month",
        default=None,
        help="End month in YYYY-MM format (default: previous month)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if data file already exists"
    )
    
    args = parser.parse_args()
    
    # Get API configuration
    api_url, api_key = get_config()
    
    # Parse date range for filtering
    start_year, start_month = parse_month(args.start_month)
    
    if args.end_month:
        end_year, end_month = parse_month(args.end_month)
    else:
        today = datetime.now()
        if today.month == 1:
            end_year, end_month = today.year - 1, 12
        else:
            end_year, end_month = today.year, today.month - 1
    
    # Setup paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data" / "fetched"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Load taxonomy caches for community classification
    print("Loading taxonomy cache...")
    taxonomy_cache, assembly_cache = load_cache()
    
    print(f"Fetching Grafana landing data (all available, filtering {start_year}-{start_month:02d} to {end_year}-{end_month:02d})")
    print("=" * 60)
    
    # Fetch all data in one query
    print("\nFetching all data from Grafana...")
    response = fetch_landing_data(api_url, api_key)
    
    if response is None:
        print("ERROR: Failed to fetch data from Grafana")
        sys.exit(1)
    
    # Parse response
    data_points = parse_influx_response(response)
    print(f"Retrieved {len(data_points)} total data points")
    
    if not data_points:
        print("No data available from Grafana")
        return
    
    # Aggregate by month
    print("Aggregating data by month...")
    monthly_data = aggregate_by_month(data_points, taxonomy_cache, assembly_cache)
    
    print(f"Found data for {len(monthly_data)} months: {sorted(monthly_data.keys())}")
    
    # Save each month to a separate file
    fetched_count = 0
    skipped_count = 0
    
    for year, month in month_iterator(start_year, start_month, end_year, end_month):
        first_day, last_day = get_month_range(year, month)
        month_name = datetime(year, month, 1).strftime("%B %Y")
        month_key = f"{year:04d}-{month:02d}"
        
        print(f"\n--- {month_name} ---")
        
        # Check if file already exists
        output_file = data_dir / f"grafana-landings-{first_day}-to-{last_day}.json"
        
        if output_file.exists() and not args.force:
            print(f"  Data file already exists: {output_file.name}")
            skipped_count += 1
            continue
        
        # Get stats for this month
        month_stats = monthly_data.get(month_key, {
            "total_landings": 0,
            "by_workflow": {},
            "by_category": {},
            "by_community": {},
            "by_dbkey": {},
            "raw_points": []
        })
        
        # Add metadata
        output_data = {
            "metadata": {
                "source": "grafana",
                "origin": "https://brc-analytics.org/",
                "start_date": first_day,
                "end_date": last_day,
                "fetched_at": datetime.now().isoformat(),
                "data_points_count": len(month_stats.get("raw_points", []))
            },
            "summary": {
                "total_landings": month_stats["total_landings"],
                "unique_workflows": len(month_stats["by_workflow"]),
                "unique_dbkeys": len(month_stats["by_dbkey"]),
            },
            "by_workflow": month_stats["by_workflow"],
            "by_category": month_stats["by_category"],
            "by_community": month_stats["by_community"],
            "by_dbkey": month_stats["by_dbkey"],
        }
        
        # Save to file
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        
        print(f"  Total landings: {month_stats['total_landings']}")
        print(f"  Saved to: {output_file.name}")
        fetched_count += 1
    
    print("\n" + "=" * 60)
    print("All done!")
    print(f"  Saved: {fetched_count} months")
    print(f"  Skipped (cached): {skipped_count} months")


if __name__ == "__main__":
    main()
