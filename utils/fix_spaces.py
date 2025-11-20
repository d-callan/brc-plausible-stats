#!/usr/bin/env python3
"""
Fix spacing issues in the tab-separated file by replacing spaces between URLs and numbers
with tab characters.
"""

import re

def fix_spaces(input_file, output_file=None):
    """
    Replace spaces between URLs and numbers with tab characters.
    If output_file is not provided, it will overwrite the input file.
    """
    if output_file is None:
        output_file = input_file

    with open(input_file, 'r') as f_in:
        lines = f_in.readlines()
    
    fixed_lines = []
    for line in lines:
        # Fix lines where there's a space between URL and number
        fixed_line = re.sub(r'(/[^\s]+) (\d+)', r'\1\t\2', line)
        fixed_lines.append(fixed_line)
    
    with open(output_file, 'w') as f_out:
        f_out.writelines(fixed_lines)
    
    print(f"Fixed spacing issues in {input_file}")

if __name__ == '__main__':
    fix_spaces('/home/dcallan-adm/Documents/brc-analytics/user-stats/10-oct-2025.tab')
