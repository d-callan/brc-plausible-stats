#!/usr/bin/env python3
"""Fix the tab file format by properly separating URL and visitor count."""

import re

def fix_format(input_file, output_file):
    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        # Read and write header
        header = f_in.readline()
        f_out.write("Page url\tVisitors\tPageviews\tBounce rate\tTime on Page\n")
        
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            
            # Split by tabs
            parts = line.split('\t')
            
            # The first part contains URL and visitor count separated by space
            url_and_visitors = parts[0].rsplit(' ', 1)
            
            if len(url_and_visitors) == 2 and len(parts) >= 4:
                url = url_and_visitors[0]
                visitors = url_and_visitors[1]
                pageviews = parts[1]
                bounce_rate = parts[2]
                time_on_page = parts[3] if len(parts) > 3 else ''
                
                f_out.write(f"{url}\t{visitors}\t{pageviews}\t{bounce_rate}\t{time_on_page}\n")

if __name__ == '__main__':
    fix_format('30-sept-2025.tab', '30-sept-2025-fixed.tab')
    print("Fixed file created: 30-sept-2025-fixed.tab")
