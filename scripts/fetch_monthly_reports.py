#!/usr/bin/env python3
"""
Fetch monthly top pages reports from Plausible.

This script fetches top pages data for each month in a specified range
and saves the data to data/fetched/.

Usage:
    python3 fetch_monthly_reports.py
    python3 fetch_monthly_reports.py --start-month 2024-10 --end-month 2025-11
"""

import argparse
import subprocess
from datetime import datetime
from pathlib import Path
import calendar


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
        description="Fetch monthly top pages reports"
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
        "--include-all-time",
        action="store_true",
        help="Also fetch all-time data (period=all)"
    )
    
    args = parser.parse_args()
    
    # Parse start month
    start_year, start_month = parse_month(args.start_month)
    
    # Default end month to previous month (current month may be incomplete)
    if args.end_month:
        end_year, end_month = parse_month(args.end_month)
    else:
        today = datetime.now()
        # Use previous month to ensure complete data
        if today.month == 1:
            end_year, end_month = today.year - 1, 12
        else:
            end_year, end_month = today.year, today.month - 1
    
    script_dir = Path(__file__).parent
    fetch_script = script_dir / "fetch_top_pages.py"
    demographics_script = script_dir / "fetch_demographics.py"
    data_dir = script_dir.parent / "data" / "fetched"
    
    print(f"Fetching monthly reports from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    print("=" * 60)
    
    fetched_files = []
    
    for year, month in month_iterator(start_year, start_month, end_year, end_month):
        first_day, last_day = get_month_range(year, month)
        month_name = datetime(year, month, 1).strftime("%B %Y")
        
        print(f"\n--- {month_name} ---")
        
        # Check if file already exists
        expected_file = data_dir / f"top-pages-{first_day}-to-{last_day}.tab"
        
        # Check if demographics files exist (we'll check just one as a proxy)
        demographics_exist = (data_dir / f"demographics-countries-{first_day}-to-{last_day}.tab").exists()
        
        if expected_file.exists():
            print(f"  Data file already exists: {expected_file.name}")
            fetched_files.append(expected_file)
            
            if demographics_exist:
                print("  Demographics files already exist")
                continue
        
        # Fetch top pages data if needed
        if not expected_file.exists():
            print(f"  Fetching pages {first_day} to {last_day}...")
            result = subprocess.run(
                ["python3", str(fetch_script), "--start", first_day, "--end", last_day],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print("  ERROR: Failed to fetch data")
                print(f"  {result.stderr}")
                continue
            
            # Extract page count from output
            for line in result.stdout.split("\n"):
                if "Retrieved" in line:
                    print(f"  {line.strip()}")
                if "Saved to:" in line:
                    print(f"  {line.strip()}")
            
            fetched_files.append(expected_file)
        
        # Fetch demographics data if needed
        if not demographics_exist:
            print(f"  Fetching demographics {first_day} to {last_day}...")
            result = subprocess.run(
                ["python3", str(demographics_script), "--start", first_day, "--end", last_day],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print("  ERROR: Failed to fetch demographics")
                print(f"  {result.stderr}")
            else:
                print("  Demographics fetched")
    
    # Fetch all-time data if requested
    if args.include_all_time:
        print("\n--- All Time ---")
        all_time_file = data_dir / "top-pages-all-time.tab"
        
        # Use custom date range from site launch (Oct 2024) to today
        # v1 API doesn't support "all" period
        today = datetime.now()
        all_time_start = "2024-10-01"
        all_time_end = today.strftime("%Y-%m-%d")
        
        if all_time_file.exists():
            print(f"  Data file already exists: {all_time_file.name}")
            print("  (Delete the file to re-fetch)")
        else:
            print(f"  Fetching all-time data ({all_time_start} to {all_time_end})...")
            result = subprocess.run(
                ["python3", str(fetch_script), 
                 "--start", all_time_start, "--end", all_time_end,
                 "--output", str(all_time_file)],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print("  ERROR: Failed to fetch all-time data")
                print(f"  {result.stderr}")
            else:
                for line in result.stdout.split("\n"):
                    if "Retrieved" in line or "Saved to:" in line:
                        print(f"  {line.strip()}")
                        
        # Always try to fetch all-time demographics if main data was requested
        # We don't check for existence because "all time" changes daily
        print(f"  Fetching all-time demographics...")
        result = subprocess.run(
            ["python3", str(demographics_script), 
             "--start", all_time_start, "--end", all_time_end],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print("  ERROR: Failed to fetch all-time demographics")
            print(f"  {result.stderr}")
        else:
            print("  All-time demographics fetched")
    
    print("\n" + "=" * 60)
    print("All done!")
    print(f"Fetched {len(fetched_files)} monthly data files")
    print("  Data files: data/fetched/")


if __name__ == "__main__":
    main()
