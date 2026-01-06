#!/usr/bin/env python3
"""
Generate a comprehensive monthly summary report from Plausible data.

This script analyzes all fetched monthly data files and produces a summary
table with page counts broken down by:
- High-level pages (home, roadmap, about, etc.)
- Organism pages, assembly pages, workflow pages
- Community categories (viruses, bacteria, fungi, vectors, protists, etc.)

Usage:
    python3 generate_monthly_summary.py
    python3 generate_monthly_summary.py --output summary.txt
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Import shared taxonomy module
from taxonomy_cache import load_cache, get_community

# Cache for taxonomy lookups
_taxonomy_cache = {}
_assembly_cache = {}


def load_taxonomy_caches():
    """Load taxonomy caches if not already loaded."""
    global _taxonomy_cache, _assembly_cache
    if not _taxonomy_cache:
        _taxonomy_cache, _assembly_cache = load_cache()


def get_assembly_taxonomy(assembly_id):
    """Get taxonomy info for an assembly from cache."""
    asm_data = _assembly_cache.get(assembly_id, {})
    tax_id = asm_data.get('tax_id')
    name = asm_data.get('name', 'Unknown')
    lineage = asm_data.get('lineage', 'Unknown')
    return (tax_id, name, lineage)


def classify_community(lineage):
    """Classify an organism into a community based on its lineage."""
    return get_community(lineage)


def parse_data_file(filepath):
    """Parse a Plausible data file and extract page statistics."""
    stats = {
        'high_level': defaultdict(lambda: {'visitors': 0, 'pageviews': 0}),
        'organism_pages': [],  # List of (tax_id, visitors, pageviews)
        'assembly_pages': [],  # List of (assembly_id, visitors, pageviews)
        'workflow_pages': [],  # List of (assembly_id, workflow_name, visitors, pageviews)
        'priority_pathogen_pages': [],
        'learn_pages': {'visitors': 0, 'pageviews': 0},
    }
    
    high_level_urls = {
        '/': 'Home',
        '/data/organisms': 'Organisms Index',
        '/data/assemblies': 'Assemblies Index',
        '/data/priority-pathogens': 'Priority Pathogens Index',
        '/roadmap': 'Roadmap',
        '/about': 'About',
        '/calendar': 'Calendar',
    }
    
    with open(filepath, 'r') as f:
        next(f)  # Skip header
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            
            url = parts[0]
            try:
                visitors = int(parts[1])
                pageviews = int(parts[2])
            except ValueError:
                continue
            
            # High-level pages
            if url in high_level_urls:
                name = high_level_urls[url]
                stats['high_level'][name]['visitors'] += visitors
                stats['high_level'][name]['pageviews'] += pageviews
            
            # Organism pages: /data/organisms/{tax_id}
            elif re.match(r'^/data/organisms/\d+$', url):
                tax_id = url.split('/')[-1]
                stats['organism_pages'].append((tax_id, visitors, pageviews))
            
            # Assembly pages: /data/assemblies/{assembly_id}
            elif re.match(r'^/data/assemblies/[^/]+$', url):
                assembly_id = url.split('/')[-1]
                stats['assembly_pages'].append((assembly_id, visitors, pageviews))
            
            # Workflow pages: /data/assemblies/{assembly_id}/workflow-{...}
            elif '/workflow-' in url:
                match = re.match(r'^/data/assemblies/([^/]+)/workflow-(.+)$', url)
                if match:
                    assembly_id = match.group(1)
                    workflow_name = match.group(2)
                    stats['workflow_pages'].append((assembly_id, workflow_name, visitors, pageviews))
            
            # Priority pathogen pages
            elif re.match(r'^/data/priority-pathogens/[^/]+$', url):
                pathogen = url.split('/')[-1]
                stats['priority_pathogen_pages'].append((pathogen, visitors, pageviews))
            
            # Learn pages
            elif url.startswith('/learn'):
                stats['learn_pages']['visitors'] += visitors
                stats['learn_pages']['pageviews'] += pageviews
    
    return stats


def aggregate_by_community(pages, get_taxonomy_func, verbose=False):
    """Aggregate page stats by community classification."""
    community_stats = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0, 'unique_ids': set()})
    
    for item in pages:
        if len(item) == 3:
            id_val, visitors, pageviews = item
        else:
            id_val, _, visitors, pageviews = item  # workflow pages have extra field
        
        tax_id, name, lineage = get_taxonomy_func(id_val)
        community = classify_community(lineage)
        
        community_stats[community]['count'] += 1
        community_stats[community]['visitors'] += visitors
        community_stats[community]['pageviews'] += pageviews
        community_stats[community]['unique_ids'].add(id_val)
        
        if verbose:
            print(f"  {id_val}: {name} -> {community}", file=sys.stderr)
    
    return community_stats


def get_month_files(data_dir):
    """Get all monthly data files sorted by date."""
    files = []
    pattern = re.compile(r'top-pages-(\d{4})-(\d{2})-\d{2}-to-(\d{4})-(\d{2})-\d{2}\.tab')
    
    for f in data_dir.glob('top-pages-*.tab'):
        match = pattern.match(f.name)
        if match:
            year, month = int(match.group(1)), int(match.group(2))
            files.append((year, month, f))
    
    files.sort(key=lambda x: (x[0], x[1]))
    return files


def format_month(year, month):
    """Format year/month as 'Mon YYYY'."""
    return datetime(year, month, 1).strftime('%b %Y')


def load_taxonomy_cache(cache_file):
    """Load taxonomy cache from file."""
    global _taxonomy_cache, _assembly_taxonomy_cache
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                _taxonomy_cache = {k: tuple(v) for k, v in data.get('taxonomy', {}).items()}
                _assembly_taxonomy_cache = {k: tuple(v) for k, v in data.get('assembly', {}).items()}
                print(f"Loaded {len(_taxonomy_cache)} taxonomy and {len(_assembly_taxonomy_cache)} assembly cache entries", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Could not load cache: {e}", file=sys.stderr)


def save_taxonomy_cache(cache_file):
    """Save taxonomy cache to file."""
    try:
        data = {
            'taxonomy': {k: list(v) for k, v in _taxonomy_cache.items()},
            'assembly': {k: list(v) for k, v in _assembly_taxonomy_cache.items()},
        }
        with open(cache_file, 'w') as f:
            json.dump(data, f)
        print(f"Saved taxonomy cache ({len(_taxonomy_cache)} + {len(_assembly_taxonomy_cache)} entries)", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not save cache: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Generate monthly summary report")
    parser.add_argument('--output', '-o', help="Output file (default: stdout)")
    parser.add_argument('--no-cache', action='store_true', help="Don't use taxonomy cache")
    parser.add_argument('--verbose', '-v', action='store_true', help="Show detailed progress")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data' / 'fetched'
    cache_file = script_dir.parent / '.taxonomy_cache.json'
    
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Load cache
    if not args.no_cache:
        load_taxonomy_cache(cache_file)
    
    # Get all monthly files
    month_files = get_month_files(data_dir)
    if not month_files:
        print("Error: No monthly data files found", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(month_files)} monthly data files", file=sys.stderr)
    
    # Collect all unique tax IDs and assembly IDs first for batch lookup
    all_tax_ids = set()
    all_assembly_ids = set()
    
    print("Scanning files for unique IDs...", file=sys.stderr)
    for year, month, filepath in month_files:
        stats = parse_data_file(filepath)
        for tax_id, _, _ in stats['organism_pages']:
            all_tax_ids.add(tax_id)
        for assembly_id, _, _ in stats['assembly_pages']:
            all_assembly_ids.add(assembly_id)
        for assembly_id, _, _, _ in stats['workflow_pages']:
            all_assembly_ids.add(assembly_id)
    
    print(f"Found {len(all_tax_ids)} unique tax IDs and {len(all_assembly_ids)} unique assembly IDs", file=sys.stderr)
    
    # Load taxonomy cache
    print("Loading taxonomy cache...", file=sys.stderr)
    load_taxonomy_caches()
    print(f"  Loaded {len(_taxonomy_cache)} taxonomy entries", file=sys.stderr)
    print(f"  Loaded {len(_assembly_cache)} assembly entries", file=sys.stderr)
    
    # Process each month
    monthly_data = []
    
    print("Processing monthly data...", file=sys.stderr)
    for year, month, filepath in month_files:
        month_label = format_month(year, month)
        print(f"  Processing {month_label}...", file=sys.stderr)
        
        stats = parse_data_file(filepath)
        
        # Aggregate organism pages by community
        org_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for tax_id, visitors, pageviews in stats['organism_pages']:
            name, lineage = _taxonomy_cache.get(tax_id, ('Unknown', 'Unknown'))
            community = classify_community(lineage)
            org_by_community[community]['count'] += 1
            org_by_community[community]['visitors'] += visitors
            org_by_community[community]['pageviews'] += pageviews
        
        # Aggregate assembly pages by community
        asm_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for assembly_id, visitors, pageviews in stats['assembly_pages']:
            _, name, lineage = _assembly_taxonomy_cache.get(assembly_id, (None, 'Unknown', 'Unknown'))
            community = classify_community(lineage)
            asm_by_community[community]['count'] += 1
            asm_by_community[community]['visitors'] += visitors
            asm_by_community[community]['pageviews'] += pageviews
        
        # Aggregate workflow pages by community
        wf_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for assembly_id, workflow, visitors, pageviews in stats['workflow_pages']:
            _, name, lineage = _assembly_taxonomy_cache.get(assembly_id, (None, 'Unknown', 'Unknown'))
            community = classify_community(lineage)
            wf_by_community[community]['count'] += 1
            wf_by_community[community]['visitors'] += visitors
            wf_by_community[community]['pageviews'] += pageviews
        
        monthly_data.append({
            'month': month_label,
            'year': year,
            'month_num': month,
            'high_level': dict(stats['high_level']),
            'organism_total': {
                'count': len(stats['organism_pages']),
                'visitors': sum(v for _, v, _ in stats['organism_pages']),
                'pageviews': sum(p for _, _, p in stats['organism_pages']),
            },
            'organism_by_community': dict(org_by_community),
            'assembly_total': {
                'count': len(stats['assembly_pages']),
                'visitors': sum(v for _, v, _ in stats['assembly_pages']),
                'pageviews': sum(p for _, _, p in stats['assembly_pages']),
            },
            'assembly_by_community': dict(asm_by_community),
            'workflow_total': {
                'count': len(stats['workflow_pages']),
                'visitors': sum(v for _, _, v, _ in stats['workflow_pages']),
                'pageviews': sum(p for _, _, _, p in stats['workflow_pages']),
            },
            'workflow_by_community': dict(wf_by_community),
            'priority_pathogens': {
                'count': len(stats['priority_pathogen_pages']),
                'visitors': sum(v for _, v, _ in stats['priority_pathogen_pages']),
                'pageviews': sum(p for _, _, p in stats['priority_pathogen_pages']),
            },
            'learn': stats['learn_pages'],
        })
    
    # Generate report
    output = []
    
    output.append("=" * 120)
    output.append("BRC ANALYTICS - MONTHLY TRAFFIC SUMMARY")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    output.append("=" * 120)
    output.append("")
    
    # Section 1: High-level pages
    output.append("HIGH-LEVEL PAGES (Visitors / Pageviews)")
    output.append("-" * 120)
    
    # Header
    header = f"{'Month':<12}"
    pages = ['Home', 'Organisms Index', 'Assemblies Index', 'Priority Pathogens Index', 'Roadmap', 'About', 'Calendar']
    for page in pages:
        header += f"{page[:15]:>18}"
    output.append(header)
    output.append("-" * 120)
    
    for data in monthly_data:
        row = f"{data['month']:<12}"
        for page in pages:
            stats = data['high_level'].get(page, {'visitors': 0, 'pageviews': 0})
            row += f"{stats['visitors']:>8}/{stats['pageviews']:<8}"
        output.append(row)
    
    output.append("")
    output.append("")
    
    # Section 2: Content pages totals
    output.append("CONTENT PAGES - TOTALS (Unique Pages / Visitors / Pageviews)")
    output.append("-" * 120)
    output.append(f"{'Month':<12}{'Organism Pages':>25}{'Assembly Pages':>25}{'Workflow Pages':>25}{'Priority Pathogens':>25}")
    output.append("-" * 120)
    
    for data in monthly_data:
        org = data['organism_total']
        asm = data['assembly_total']
        wf = data['workflow_total']
        pp = data['priority_pathogens']
        row = f"{data['month']:<12}"
        row += f"{org['count']:>6} / {org['visitors']:>5} / {org['pageviews']:<6}"
        row += f"{asm['count']:>6} / {asm['visitors']:>5} / {asm['pageviews']:<6}"
        row += f"{wf['count']:>6} / {wf['visitors']:>5} / {wf['pageviews']:<6}"
        row += f"{pp['count']:>6} / {pp['visitors']:>5} / {pp['pageviews']:<6}"
        output.append(row)
    
    output.append("")
    output.append("")
    
    # Section 3: Organism pages by community
    communities = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths', 'Other']
    
    output.append("ORGANISM PAGES BY COMMUNITY (Unique Pages / Visitors)")
    output.append("-" * 120)
    header = f"{'Month':<12}"
    for comm in communities:
        header += f"{comm:>14}"
    output.append(header)
    output.append("-" * 120)
    
    for data in monthly_data:
        row = f"{data['month']:<12}"
        for comm in communities:
            stats = data['organism_by_community'].get(comm, {'count': 0, 'visitors': 0})
            row += f"{stats['count']:>5}/{stats['visitors']:<7}"
        output.append(row)
    
    output.append("")
    output.append("")
    
    # Section 4: Assembly pages by community
    output.append("ASSEMBLY PAGES BY COMMUNITY (Unique Pages / Visitors)")
    output.append("-" * 120)
    header = f"{'Month':<12}"
    for comm in communities:
        header += f"{comm:>14}"
    output.append(header)
    output.append("-" * 120)
    
    for data in monthly_data:
        row = f"{data['month']:<12}"
        for comm in communities:
            stats = data['assembly_by_community'].get(comm, {'count': 0, 'visitors': 0})
            row += f"{stats['count']:>5}/{stats['visitors']:<7}"
        output.append(row)
    
    output.append("")
    output.append("")
    
    # Section 5: Workflow pages by community
    output.append("WORKFLOW PAGES BY COMMUNITY (Unique Pages / Visitors)")
    output.append("-" * 120)
    header = f"{'Month':<12}"
    for comm in communities:
        header += f"{comm:>14}"
    output.append(header)
    output.append("-" * 120)
    
    for data in monthly_data:
        row = f"{data['month']:<12}"
        for comm in communities:
            stats = data['workflow_by_community'].get(comm, {'count': 0, 'visitors': 0})
            row += f"{stats['count']:>5}/{stats['visitors']:<7}"
        output.append(row)
    
    output.append("")
    output.append("")
    
    # Section 6: Learn pages
    output.append("LEARN / FEATURED ANALYSES PAGES")
    output.append("-" * 50)
    output.append(f"{'Month':<12}{'Visitors':>12}{'Pageviews':>12}")
    output.append("-" * 50)
    
    for data in monthly_data:
        learn = data['learn']
        output.append(f"{data['month']:<12}{learn['visitors']:>12}{learn['pageviews']:>12}")
    
    output.append("")
    output.append("")
    output.append("=" * 120)
    output.append("NOTES:")
    output.append("- 'Organism Pages' = /data/organisms/{tax_id} (individual organism detail pages)")
    output.append("- 'Assembly Pages' = /data/assemblies/{assembly_id} (individual assembly detail pages)")
    output.append("- 'Workflow Pages' = /data/assemblies/{id}/workflow-{...} (workflow configuration pages)")
    output.append("- Index pages (Organisms Index, etc.) are navigation/listing pages, not detail pages")
    output.append("- Community classification based on NCBI taxonomy lineage")
    output.append("=" * 120)
    
    # Output
    report = '\n'.join(output)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
