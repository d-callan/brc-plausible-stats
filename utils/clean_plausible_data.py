#!/usr/bin/env python3
"""
Clean and format Plausible Analytics export data.

This script handles various formatting issues from Plausible exports:
- URLs and data on separate lines (standard Plausible export format)
- Spaces between URLs and numbers instead of tabs
- Missing or malformed tab separators

Usage:
    python clean_plausible_data.py <input_file> [output_file]
    
If output_file is not provided, creates a file with '-cleaned' suffix.
"""

import sys
import re
import os


def clean_plausible_data(input_file, output_file=None):
    """
    Clean Plausible Analytics export data into proper tab-separated format.
    
    Args:
        input_file: Path to the input file
        output_file: Path to the output file (optional)
    
    Returns:
        Path to the output file
    """
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}-cleaned{ext}"
    
    with open(input_file, 'r') as f_in:
        lines = f_in.readlines()
    
    cleaned_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Handle header line
        if i == 0 or line.startswith('Page url'):
            cleaned_lines.append("Page url\tVisitors\tPageviews\tBounce rate\tTime on Page\n")
            i += 1
            continue
        
        # Check if this is a URL line (starts with / or is a special case)
        if line.startswith('/') or line in ['/', '/)']:
            url = line
            
            # Look ahead for the data line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                
                # Check if next line has the data (starts with a number or has tabs)
                if next_line and (next_line[0].isdigit() or '\t' in next_line):
                    # Parse the data line
                    parts = next_line.split('\t')
                    
                    if len(parts) >= 4:
                        # Data is already tab-separated
                        visitors = parts[0]
                        pageviews = parts[1]
                        bounce_rate = parts[2]
                        time_on_page = parts[3] if len(parts) > 3 else '-'
                    else:
                        # Data might be space-separated or mixed
                        # Try to parse: visitors pageviews bounce_rate time_on_page
                        data_parts = next_line.split()
                        if len(data_parts) >= 3:
                            visitors = data_parts[0]
                            pageviews = data_parts[1]
                            bounce_rate = data_parts[2]
                            # Time on page might be multiple parts (e.g., "2m 56s")
                            time_on_page = ' '.join(data_parts[3:]) if len(data_parts) > 3 else '-'
                        else:
                            # Malformed data, skip
                            i += 2
                            continue
                    
                    # Write the cleaned line
                    cleaned_lines.append(f"{url}\t{visitors}\t{pageviews}\t{bounce_rate}\t{time_on_page}\n")
                    i += 2  # Skip both URL and data lines
                    continue
        
        # Handle case where URL and data are on the same line (already formatted or space-separated)
        if '\t' in line:
            # Already tab-separated, just clean it up
            parts = line.split('\t')
            if len(parts) >= 5:
                cleaned_lines.append(line + '\n' if not line.endswith('\n') else line)
            i += 1
            continue
        
        # Handle space-separated format: /url visitors pageviews bounce_rate time
        if line.startswith('/'):
            # Try to split by spaces
            match = re.match(r'(/[^\s]+)\s+(\d+)\s+(\d+)\s+([^\s]+)\s+(.*)', line)
            if match:
                url, visitors, pageviews, bounce_rate, time_on_page = match.groups()
                time_on_page = time_on_page.strip() if time_on_page.strip() else '-'
                cleaned_lines.append(f"{url}\t{visitors}\t{pageviews}\t{bounce_rate}\t{time_on_page}\n")
                i += 1
                continue
        
        # If we can't parse it, skip it
        i += 1
    
    # Write the cleaned data
    with open(output_file, 'w') as f_out:
        f_out.writelines(cleaned_lines)
    
    return output_file


def main():
    if len(sys.argv) < 2:
        print("Usage: python clean_plausible_data.py <input_file> [output_file]")
        print("\nCleans Plausible Analytics export data into proper tab-separated format.")
        print("If output_file is not provided, creates a file with '-cleaned' suffix.")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    
    try:
        result_file = clean_plausible_data(input_file, output_file)
        print(f"Successfully cleaned data!")
        print(f"Input:  {input_file}")
        print(f"Output: {result_file}")
        
        # Show statistics
        with open(result_file, 'r') as f:
            lines = f.readlines()
            data_lines = len([l for l in lines if l.strip() and not l.startswith('Page url')])
            print(f"\nProcessed {data_lines} data rows")
    
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
