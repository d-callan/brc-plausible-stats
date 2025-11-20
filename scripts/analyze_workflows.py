#!/usr/bin/env python3
"""
Analyze workflow configuration page visits from web usage data.
Filters for workflow pages and summarizes key statistics.
"""

import re
import sys
import json
import time
import subprocess
from collections import defaultdict
from statistics import median, mean


def parse_time(time_str):
    """Convert time string like '7m 38s' or '17s' to seconds."""
    if not time_str or time_str == '-':
        return None
    
    seconds = 0
    # Match minutes
    m_match = re.search(r'(\d+)m', time_str)
    if m_match:
        seconds += int(m_match.group(1)) * 60
    
    # Match seconds
    s_match = re.search(r'(\d+)s', time_str)
    if s_match:
        seconds += int(s_match.group(1))
    
    return seconds


def extract_assembly_id(url):
    """Extract assembly ID from workflow URL."""
    # Pattern: /data/assemblies/{ASSEMBLY_ID}/workflow-...
    match = re.search(r'/data/assemblies/([^/]+)/workflow-', url)
    if match:
        return match.group(1)
    return None


def parse_data_file(filename):
    """Parse the tab-separated data file."""
    workflow_data = []
    
    with open(filename, 'r') as f:
        # Skip header
        next(f)
        
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) < 5:
                continue
            
            url = parts[0]
            
            # Filter for workflow configuration pages
            if '/workflow-' not in url:
                continue
            
            # Extract data
            try:
                visitors = int(parts[1])
                pageviews = int(parts[2])
                time_on_page = parse_time(parts[4])
                assembly_id = extract_assembly_id(url)
                
                if assembly_id:
                    workflow_data.append({
                        'url': url,
                        'assembly': assembly_id,
                        'visitors': visitors,
                        'pageviews': pageviews,
                        'time_on_page': time_on_page
                    })
            except (ValueError, IndexError):
                continue
    
    return workflow_data


def extract_workflow_name(url):
    """Extract workflow name from URL."""
    # Pattern: /workflow-github-com-iwc-workflows-{WORKFLOW_NAME}-...
    match = re.search(r'/workflow-github-com-iwc-workflows-([^-]+(?:-[^-]+)*?)-(?:main|versions)', url)
    if match:
        return match.group(1)
    return 'unknown'


def summarize_workflows(workflow_data):
    """Generate summary statistics for workflow pages."""
    # Group by assembly
    assembly_stats = defaultdict(lambda: {
        'visitors': 0,
        'pageviews': 0,
        'times': []
    })
    
    # Group by workflow type
    workflow_stats = defaultdict(lambda: {
        'visitors': 0,
        'pageviews': 0,
        'times': [],
        'assemblies': set()
    })
    
    # Group by workflow-assembly combination
    workflow_assembly_stats = defaultdict(lambda: {
        'visitors': 0,
        'pageviews': 0,
        'times': []
    })
    
    for entry in workflow_data:
        assembly = entry['assembly']
        assembly_stats[assembly]['visitors'] += entry['visitors']
        assembly_stats[assembly]['pageviews'] += entry['pageviews']
        if entry['time_on_page'] is not None:
            assembly_stats[assembly]['times'].append(entry['time_on_page'])
        
        # Extract and group by workflow
        workflow_name = extract_workflow_name(entry['url'])
        workflow_stats[workflow_name]['visitors'] += entry['visitors']
        workflow_stats[workflow_name]['pageviews'] += entry['pageviews']
        workflow_stats[workflow_name]['assemblies'].add(assembly)
        if entry['time_on_page'] is not None:
            workflow_stats[workflow_name]['times'].append(entry['time_on_page'])
        
        # Group by workflow-assembly combination
        combo_key = (workflow_name, assembly)
        workflow_assembly_stats[combo_key]['visitors'] += entry['visitors']
        workflow_assembly_stats[combo_key]['pageviews'] += entry['pageviews']
        if entry['time_on_page'] is not None:
            workflow_assembly_stats[combo_key]['times'].append(entry['time_on_page'])
    
    # Overall statistics
    total_visitors = sum(entry['visitors'] for entry in workflow_data)
    total_pageviews = sum(entry['pageviews'] for entry in workflow_data)
    all_times = [entry['time_on_page'] for entry in workflow_data if entry['time_on_page'] is not None]
    
    return assembly_stats, workflow_stats, workflow_assembly_stats, total_visitors, total_pageviews, all_times


def format_time(seconds):
    """Format seconds as human-readable time."""
    if seconds is None:
        return 'N/A'
    
    minutes = seconds // 60
    secs = seconds % 60
    
    if minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


def get_organism_name(assembly_id):
    """Fetch organism name from NCBI API using curl."""
    # Strip version number from end (e.g., GCA_001008285_1 -> GCA_001008285)
    if '_' in assembly_id:
        accession = assembly_id.rsplit('_', 1)[0]
    else:
        accession = assembly_id
    
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/{accession}/dataset_report"
    
    try:
        # Use curl to fetch data (urllib seems to have issues with this API)
        result = subprocess.run(
            ['curl', '-s', '-H', 'accept: application/json', url],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout:
            return 'Unknown'
        
        data = json.loads(result.stdout)
        
        if 'reports' in data and len(data['reports']) > 0:
            report = data['reports'][0]
            if 'organism' in report:
                organism = report['organism']
                # Prefer common_name, fall back to organism_name or sci_name
                return (organism.get('common_name') or 
                       organism.get('organism_name') or 
                       organism.get('sci_name') or 
                       'Unknown')
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, Exception) as e:
        print(f"Warning: Could not fetch organism for {assembly_id}: {e}", file=sys.stderr)
        return 'Unknown'
    
    return 'Unknown'


def fetch_organism_names(assembly_ids):
    """Fetch organism names for all assemblies."""
    organism_map = {}
    total = len(assembly_ids)
    
    print(f"Fetching organism names for {total} assemblies...", file=sys.stderr)
    
    for i, assembly_id in enumerate(assembly_ids, 1):
        print(f"  [{i}/{total}] {assembly_id}...", file=sys.stderr)
        organism_map[assembly_id] = get_organism_name(assembly_id)
        # Add delay to avoid rate limiting (NCBI allows ~3 requests per second)
        time.sleep(0.35)
    
    print("Done fetching organism names.\n", file=sys.stderr)
    return organism_map


def write_output(output_file, workflow_data, assembly_stats, workflow_stats, workflow_assembly_stats, total_visitors, total_pageviews, all_times, organism_map):
    """Write analysis results to output file."""
    # Assemblies with potential first-in-list bias
    biased_assemblies = {'GCA_001008285_1', 'GCA_000826245_1'}
    
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("WORKFLOW CONFIGURATION PAGE ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Found {len(workflow_data)} workflow configuration page entries\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total unique assemblies with workflow visits: {len(assembly_stats)}\n")
        f.write(f"Total unique workflows: {len(workflow_stats)}\n")
        f.write(f"Total visitors to workflow pages: {total_visitors}\n")
        f.write(f"Total pageviews: {total_pageviews}\n")
        
        if all_times:
            avg_time = mean(all_times)
            median_time = median(all_times)
            f.write(f"Average time on page: {format_time(int(avg_time))}\n")
            f.write(f"Median time on page: {format_time(int(median_time))}\n")
        else:
            f.write("Average time on page: N/A\n")
            f.write("Median time on page: N/A\n")
        
        f.write("\n\n")
        
        # Per-workflow breakdown
        f.write("PER-WORKFLOW BREAKDOWN\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Workflow':<35} {'Visitors':<10} {'Pageviews':<10} {'Assemblies':<12} {'Avg Time':<12} {'Median Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        # Sort by visitors (descending)
        sorted_workflows = sorted(workflow_stats.items(), 
                                 key=lambda x: x[1]['visitors'], 
                                 reverse=True)
        
        for workflow, stats in sorted_workflows:
            visitors = stats['visitors']
            pageviews = stats['pageviews']
            num_assemblies = len(stats['assemblies'])
            
            # Truncate workflow name if too long
            workflow_display = workflow
            if len(workflow_display) > 33:
                workflow_display = workflow_display[:30] + "..."
            
            if stats['times']:
                avg_time = format_time(int(mean(stats['times'])))
                med_time = format_time(int(median(stats['times'])))
            else:
                avg_time = 'N/A'
                med_time = 'N/A'
            
            f.write(f"{workflow_display:<35} {visitors:<10} {pageviews:<10} {num_assemblies:<12} {avg_time:<12} {med_time:<12}\n")
        
        f.write("\n\n")
        
        # Workflow-Organism intersections
        f.write("WORKFLOW-ORGANISM INTERSECTIONS (Top 20)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Workflow':<30} {'Organism':<30} {'Visitors':<10} {'Pageviews':<10}\n")
        f.write("-" * 80 + "\n")
        
        # Sort by visitors (descending) and take top 20
        sorted_combos = sorted(workflow_assembly_stats.items(), 
                              key=lambda x: x[1]['visitors'], 
                              reverse=True)[:20]
        
        for (workflow, assembly), stats in sorted_combos:
            visitors = stats['visitors']
            pageviews = stats['pageviews']
            organism = organism_map.get(assembly, 'Unknown')
            
            # Truncate names if too long
            workflow_display = workflow
            if len(workflow_display) > 28:
                workflow_display = workflow_display[:25] + "..."
            
            organism_display = organism
            if len(organism_display) > 28:
                organism_display = organism_display[:25] + "..."
            
            f.write(f"{workflow_display:<30} {organism_display:<30} {visitors:<10} {pageviews:<10}\n")
        
        f.write("\n\n")
        
        # Per-assembly breakdown
        f.write("PER-ASSEMBLY BREAKDOWN\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Assembly ID':<20} {'Organism':<30} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12} {'Median Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        # Sort by visitors (descending)
        sorted_assemblies = sorted(assembly_stats.items(), 
                                  key=lambda x: x[1]['visitors'], 
                                  reverse=True)
        
        for assembly, stats in sorted_assemblies:
            visitors = stats['visitors']
            pageviews = stats['pageviews']
            organism = organism_map.get(assembly, 'Unknown')
            
            # Truncate organism name if too long
            if len(organism) > 28:
                organism = organism[:25] + "..."
            
            if stats['times']:
                avg_time = format_time(int(mean(stats['times'])))
                med_time = format_time(int(median(stats['times'])))
            else:
                avg_time = 'N/A'
                med_time = 'N/A'
            
            # Add bias indicator
            bias_marker = " *" if assembly in biased_assemblies else "  "
            f.write(f"{assembly:<20} {organism:<30} {visitors:<10} {pageviews:<10} {avg_time:<12} {med_time:<12}{bias_marker}\n")
        
        f.write("\n")
        f.write("* = May have first-in-list bias (appears early in assembly listings)\n")
        f.write("\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_workflows.py <data_file> [output_file]")
        sys.exit(1)
    
    filename = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else filename.replace('.tab', '-workflow-analysis.txt')
    
    # Parse data
    workflow_data = parse_data_file(filename)
    
    if not workflow_data:
        print("No workflow pages found in the data.")
        return
    
    # Generate summary
    assembly_stats, workflow_stats, workflow_assembly_stats, total_visitors, total_pageviews, all_times = summarize_workflows(workflow_data)
    
    # Fetch organism names
    assembly_ids = list(assembly_stats.keys())
    organism_map = fetch_organism_names(assembly_ids)
    
    # Write to file
    write_output(output_file, workflow_data, assembly_stats, workflow_stats, workflow_assembly_stats, total_visitors, total_pageviews, all_times, organism_map)
    
    print(f"Analysis complete! Results written to: {output_file}")
    print("\nSummary:")
    print(f"  - {len(workflow_data)} workflow configuration page entries")
    print(f"  - {len(assembly_stats)} unique assemblies")
    print(f"  - {len(workflow_stats)} unique workflows")
    print(f"  - {total_visitors} total visitors")
    print(f"  - {total_pageviews} total pageviews")


if __name__ == '__main__':
    main()
