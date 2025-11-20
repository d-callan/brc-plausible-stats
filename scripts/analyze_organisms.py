#!/usr/bin/env python3
"""
Analyze organism and priority pathogen page visits from web usage data.

Note: "Without assembly/workflow" sections identify pages where the site offers
these features but they were not visited during the reporting period.
"""

import re
import sys
import json
import time
import subprocess


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


def get_assemblies_for_taxon(tax_id):
    """Fetch list of assemblies for a given taxonomy ID from NCBI API."""
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/taxon/{tax_id}/dataset_report"
    
    try:
        result = subprocess.run(
            ['curl', '-s', '-H', 'accept: application/json', url],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout or len(result.stdout) < 10:
            return []
        
        data = json.loads(result.stdout)
        
        assemblies = []
        if 'reports' in data:
            for report in data['reports']:
                if 'accession' in report:
                    # Convert accession format (e.g., GCA_001008285.1 -> GCA_001008285_1)
                    accession = report['accession'].replace('.', '_')
                    assemblies.append(accession)
        
        return assemblies
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, Exception):
        return []


def parse_data_file(filename):
    """Parse the tab-separated data file."""
    organism_pages = []
    priority_pathogen_pages = []
    assembly_pages_all = []
    assembly_pages_no_workflow = []
    landing_pages = []
    
    # First pass: collect all data
    all_urls = {}
    with open(filename, 'r') as f:
        # Skip header
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) < 5:  # We expect at least 5 parts: URL, visitors, pageviews, bounce rate, time
                continue
            
            url = parts[0]
            
            # Extract data
            try:
                visitors = int(parts[1])
                pageviews = int(parts[2])
                time_on_page = parse_time(parts[4])
                
                all_urls[url] = {
                    'visitors': visitors,
                    'pageviews': pageviews,
                    'time_on_page': time_on_page
                }
                
                # Filter for organism pages
                if re.match(r'^/data/organisms/\d+$', url):
                    organism_id = url.split('/')[-1]
                    organism_pages.append({
                        'url': url,
                        'organism_id': organism_id,
                        'visitors': visitors,
                        'pageviews': pageviews,
                        'time_on_page': time_on_page
                    })
                
                # Filter for priority pathogen pages
                elif re.match(r'^/data/priority-pathogens/[^/]+$', url):
                    pathogen_name = url.split('/')[-1]
                    priority_pathogen_pages.append({
                        'url': url,
                        'pathogen_name': pathogen_name,
                        'visitors': visitors,
                        'pageviews': pageviews,
                        'time_on_page': time_on_page
                    })
                
                # Filter for assembly pages (base URL only)
                elif re.match(r'^/data/assemblies/[^/]+$', url):
                    assembly_id = url.split('/')[-1]
                    assembly_pages_all.append({
                        'url': url,
                        'assembly_id': assembly_id,
                        'visitors': visitors,
                        'pageviews': pageviews,
                        'time_on_page': time_on_page
                    })
                
                # Filter for high-level landing/navigation pages
                elif url in ['/', '/data/organisms', '/data/assemblies', '/data/priority-pathogens', '/roadmap', '/about']:
                    landing_pages.append({
                        'url': url,
                        'visitors': visitors,
                        'pageviews': pageviews,
                        'time_on_page': time_on_page
                    })
                    
            except (ValueError, IndexError):
                continue
    
    # Second pass: identify assemblies without workflow URLs
    assemblies_with_workflows = set()
    for url in all_urls.keys():
        if '/workflow-' in url:
            # Extract assembly ID from workflow URL
            match = re.match(r'^/data/assemblies/([^/]+)/workflow-', url)
            if match:
                assemblies_with_workflows.add(match.group(1))
    
    # Filter assembly pages to those without workflows
    for page in assembly_pages_all:
        if page['assembly_id'] not in assemblies_with_workflows:
            assembly_pages_no_workflow.append(page)
    
    return organism_pages, priority_pathogen_pages, assembly_pages_all, assembly_pages_no_workflow, landing_pages


def get_organism_name_from_taxid(tax_id):
    """Fetch organism name from NCBI Taxonomy API using tax ID."""
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/taxonomy/taxon/{tax_id}"
    
    try:
        result = subprocess.run(
            ['curl', '-s', '-H', 'accept: application/json', url],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout:
            return 'Unknown'
        
        data = json.loads(result.stdout)
        
        if 'taxonomy_nodes' in data and len(data['taxonomy_nodes']) > 0:
            node = data['taxonomy_nodes'][0]
            if 'taxonomy' in node:
                taxonomy = node['taxonomy']
                return (taxonomy.get('common_name') or 
                       taxonomy.get('organism_name') or 
                       taxonomy.get('sci_name') or 
                       'Unknown')
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, Exception):
        return 'Unknown'
    
    return 'Unknown'


def get_organism_name_from_assembly(assembly_id):
    """Fetch organism name from NCBI API using assembly ID."""
    # Strip version number from end
    if '_' in assembly_id:
        accession = assembly_id.rsplit('_', 1)[0]
    else:
        accession = assembly_id
    
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/{accession}/dataset_report"
    
    try:
        result = subprocess.run(
            ['curl', '-s', '-H', 'accept: application/json', url],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout or len(result.stdout) < 10:
            return 'Unknown'
        
        data = json.loads(result.stdout)
        
        if 'reports' in data and len(data['reports']) > 0:
            report = data['reports'][0]
            if 'organism' in report:
                organism = report['organism']
                return (organism.get('common_name') or 
                       organism.get('organism_name') or 
                       organism.get('sci_name') or 
                       'Unknown')
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, Exception):
        return 'Unknown'
    
    return 'Unknown'


def fetch_organism_names(organism_ids):
    """Fetch organism names for all tax IDs."""
    organism_map = {}
    total = len(organism_ids)
    
    print(f"Fetching organism names for {total} tax IDs...", file=sys.stderr)
    
    for i, tax_id in enumerate(organism_ids, 1):
        print(f"  [{i}/{total}] Tax ID {tax_id}...", file=sys.stderr)
        organism_map[tax_id] = get_organism_name_from_taxid(tax_id)
        time.sleep(0.35)
    
    print("Done fetching organism names.\n", file=sys.stderr)
    return organism_map


def fetch_assembly_names(assembly_ids):
    """Fetch organism names for all assemblies."""
    assembly_map = {}
    total = len(assembly_ids)
    
    print(f"Fetching organism names for {total} assemblies...", file=sys.stderr)
    
    for i, assembly_id in enumerate(assembly_ids, 1):
        print(f"  [{i}/{total}] {assembly_id}...", file=sys.stderr)
        assembly_map[assembly_id] = get_organism_name_from_assembly(assembly_id)
        time.sleep(0.35)
    
    print("Done fetching assembly names.\n", file=sys.stderr)
    return assembly_map


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


def write_output(output_file, organism_pages, organism_pages_no_assembly, priority_pathogen_pages, assembly_pages_all, assembly_pages_no_workflow, landing_pages, organism_map, assembly_map):
    """Write analysis results to output file."""
    # Assemblies with potential first-in-list bias
    biased_assemblies = {'GCA_001008285_1', 'GCA_000826245_1'}
    
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("ORGANISM AND PATHOGEN PAGE ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
        
        # Overall statistics
        total_organism_visitors = sum(p['visitors'] for p in organism_pages)
        total_organism_pageviews = sum(p['pageviews'] for p in organism_pages)
        total_organism_no_asm_visitors = sum(p['visitors'] for p in organism_pages_no_assembly)
        total_organism_no_asm_pageviews = sum(p['pageviews'] for p in organism_pages_no_assembly)
        total_pathogen_visitors = sum(p['visitors'] for p in priority_pathogen_pages)
        total_pathogen_pageviews = sum(p['pageviews'] for p in priority_pathogen_pages)
        total_assembly_visitors = sum(p['visitors'] for p in assembly_pages_all)
        total_assembly_pageviews = sum(p['pageviews'] for p in assembly_pages_all)
        total_assembly_no_wf_visitors = sum(p['visitors'] for p in assembly_pages_no_workflow)
        total_assembly_no_wf_pageviews = sum(p['pageviews'] for p in assembly_pages_no_workflow)
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Organism pages (all): {len(organism_pages)} unique, {total_organism_visitors} visitors, {total_organism_pageviews} pageviews\n")
        f.write(f"Organism pages (with no assembly page visits): {len(organism_pages_no_assembly)} unique, {total_organism_no_asm_visitors} visitors, {total_organism_no_asm_pageviews} pageviews\n")
        f.write(f"Priority pathogen pages: {len(priority_pathogen_pages)} unique, {total_pathogen_visitors} visitors, {total_pathogen_pageviews} pageviews\n")
        f.write(f"Assembly pages (all): {len(assembly_pages_all)} unique, {total_assembly_visitors} visitors, {total_assembly_pageviews} pageviews\n")
        f.write(f"Assembly pages (with no workflow page visits): {len(assembly_pages_no_workflow)} unique, {total_assembly_no_wf_visitors} visitors, {total_assembly_no_wf_pageviews} pageviews\n")
        f.write("\n\n")
        
        # Landing/Navigation pages
        f.write("HIGH-LEVEL NAVIGATION PAGES\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Page':<40} {'Visitors':<10} {'Pageviews':<10} {'Bounce Rate':<12} {'Avg Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        # Sort by visitors (descending)
        sorted_landing = sorted(landing_pages, 
                               key=lambda x: x['visitors'], 
                               reverse=True)
        
        for page in sorted_landing:
            url = page['url']
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            # Calculate bounce rate (visitors who view only this page)
            if pageviews > 0:
                bounce_rate = f"{int((visitors / pageviews) * 100)}%"
            else:
                bounce_rate = "N/A"
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            f.write(f"{url:<40} {visitors:<10} {pageviews:<10} {bounce_rate:<12} {time_str:<12}\n")
        
        f.write("\n\n")
        
        # Priority Pathogens breakdown
        f.write("PRIORITY PATHOGEN PAGES\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Pathogen':<40} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12} {'Median Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        sorted_pathogens = sorted(priority_pathogen_pages, 
                                 key=lambda x: x['visitors'], 
                                 reverse=True)
        
        for page in sorted_pathogens:
            pathogen = page['pathogen_name'].replace('-', ' ').title()
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            f.write(f"{pathogen:<40} {visitors:<10} {pageviews:<10} {time_str:<12} {time_str:<12}\n")
        
        f.write("\n\n")
        
        # Organism pages breakdown - ALL
        f.write("ORGANISM PAGES (All - Regardless of Assembly Status)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Tax ID':<15} {'Organism':<35} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        sorted_organisms = sorted(organism_pages, 
                                 key=lambda x: x['visitors'], 
                                 reverse=True)
        
        for page in sorted_organisms:
            tax_id = page['organism_id']
            organism = organism_map.get(tax_id, 'Unknown')
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            # Truncate organism name if too long
            if len(organism) > 33:
                organism = organism[:30] + "..."
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            f.write(f"{tax_id:<15} {organism:<35} {visitors:<10} {pageviews:<10} {time_str:<12}\n")
        
        f.write("\n\n")
        
        # Organism pages breakdown - NO ASSEMBLY VISITS
        f.write("ORGANISM PAGES (Where Available Assembly Pages Were Not Visited)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Tax ID':<15} {'Organism':<35} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        sorted_organisms_no_asm = sorted(organism_pages_no_assembly, 
                                        key=lambda x: x['visitors'], 
                                        reverse=True)
        
        for page in sorted_organisms_no_asm:
            tax_id = page['organism_id']
            organism = organism_map.get(tax_id, 'Unknown')
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            # Truncate organism name if too long
            if len(organism) > 33:
                organism = organism[:30] + "..."
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            f.write(f"{tax_id:<15} {organism:<35} {visitors:<10} {pageviews:<10} {time_str:<12}\n")
        
        f.write("\n\n")
        
        # Assembly pages breakdown - ALL
        f.write("ASSEMBLY PAGES (All - Regardless of Workflow Status)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Assembly ID':<25} {'Organism':<35} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        sorted_assemblies_all = sorted(assembly_pages_all, 
                                      key=lambda x: x['visitors'], 
                                      reverse=True)[:20]  # Top 20
        
        for page in sorted_assemblies_all:
            assembly_id = page['assembly_id']
            organism = assembly_map.get(assembly_id, 'Unknown')
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            # Truncate organism name if too long
            if len(organism) > 33:
                organism = organism[:30] + "..."
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            # Add bias indicator
            bias_marker = " *" if assembly_id in biased_assemblies else "  "
            f.write(f"{assembly_id:<25} {organism:<35} {visitors:<10} {pageviews:<10} {time_str:<12}{bias_marker}\n")
        
        f.write("\n\n")
        
        # Assembly pages breakdown - NO WORKFLOW VISITS
        f.write("ASSEMBLY PAGES (Where Available Workflow Pages Were Not Visited)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Assembly ID':<25} {'Organism':<35} {'Visitors':<10} {'Pageviews':<10} {'Avg Time':<12}\n")
        f.write("-" * 80 + "\n")
        
        sorted_assemblies_no_wf = sorted(assembly_pages_no_workflow, 
                                        key=lambda x: x['visitors'], 
                                        reverse=True)[:20]  # Top 20
        
        for page in sorted_assemblies_no_wf:
            assembly_id = page['assembly_id']
            organism = assembly_map.get(assembly_id, 'Unknown')
            visitors = page['visitors']
            pageviews = page['pageviews']
            
            # Truncate organism name if too long
            if len(organism) > 33:
                organism = organism[:30] + "..."
            
            if page['time_on_page'] is not None:
                time_str = format_time(page['time_on_page'])
            else:
                time_str = 'N/A'
            
            # Add bias indicator
            bias_marker = " *" if assembly_id in biased_assemblies else "  "
            f.write(f"{assembly_id:<25} {organism:<35} {visitors:<10} {pageviews:<10} {time_str:<12}{bias_marker}\n")
        
        f.write("\n")
        f.write("* = May have first-in-list bias (appears early in assembly listings)\n")
        f.write("\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_organisms.py <data_file> [output_file]")
        sys.exit(1)
    
    filename = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else filename.replace('.tab', '-organism-analysis.txt')
    
    # Parse data
    organism_pages, priority_pathogen_pages, assembly_pages_all, assembly_pages_no_workflow, landing_pages = parse_data_file(filename)
    
    print(f"Found {len(organism_pages)} organism pages", file=sys.stderr)
    print(f"Found {len(priority_pathogen_pages)} priority pathogen pages", file=sys.stderr)
    print(f"Found {len(assembly_pages_all)} assembly pages (all)", file=sys.stderr)
    print(f"Found {len(assembly_pages_no_workflow)} assembly pages (without workflow)\n", file=sys.stderr)
    
    # Fetch organism names for tax IDs
    organism_ids = [p['organism_id'] for p in organism_pages]
    organism_map = fetch_organism_names(organism_ids)
    
    # Check which organisms have assembly pages in our data
    print("Checking which organisms have assembly pages in the data...", file=sys.stderr)
    organisms_with_assembly_pages = set()
    
    # Get all assembly IDs from our data
    assembly_ids_in_data = {p['assembly_id'] for p in assembly_pages_all}
    
    # For each organism, check if any of its assemblies appear in our data
    for i, tax_id in enumerate(organism_ids, 1):
        print(f"  [{i}/{len(organism_ids)}] Checking tax ID {tax_id}...", file=sys.stderr)
        assemblies = get_assemblies_for_taxon(tax_id)
        
        # Check if any of this organism's assemblies are in our data
        for assembly in assemblies:
            if assembly in assembly_ids_in_data:
                organisms_with_assembly_pages.add(tax_id)
                break
        
        time.sleep(0.35)  # Rate limiting
    
    print(f"Done. Found {len(organisms_with_assembly_pages)} organisms with assembly pages in data.\n", file=sys.stderr)
    
    # Filter organism pages to those without assembly pages in our data
    organism_pages_no_assembly = [p for p in organism_pages if p['organism_id'] not in organisms_with_assembly_pages]
    
    # Fetch organism names for assemblies
    assembly_ids = [p['assembly_id'] for p in assembly_pages_all]
    assembly_map = fetch_assembly_names(assembly_ids)
    
    # Write to file
    write_output(output_file, organism_pages, organism_pages_no_assembly, priority_pathogen_pages, assembly_pages_all, assembly_pages_no_workflow, landing_pages, organism_map, assembly_map)
    
    print(f"Analysis complete! Results written to: {output_file}")
    print("\nSummary:")
    print(f"  - {len(organism_pages)} organism pages (all)")
    print(f"  - {len(organism_pages_no_assembly)} organism pages (with no assembly page visits)")
    print(f"  - {len(priority_pathogen_pages)} priority pathogen pages")
    print(f"  - {len(assembly_pages_all)} assembly pages (all)")
    print(f"  - {len(assembly_pages_no_workflow)} assembly pages (with no workflow page visits)")


if __name__ == '__main__':
    main()
