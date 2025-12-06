#!/usr/bin/env python3
"""
Generate an HTML monthly summary report with interactive charts.

This script analyzes all fetched monthly data files and produces an HTML report
with line charts showing trends over time for:
- High-level pages (home, roadmap, about, etc.)
- Content pages (organism, assembly, workflow)
- Community breakdowns (viruses, bacteria, fungi, etc.)

Usage:
    python3 generate_monthly_summary_html.py
    python3 generate_monthly_summary_html.py --output report.html
"""

import argparse
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# Community classification based on taxonomic lineage
COMMUNITY_PATTERNS = {
    'Viruses': ['Viruses', 'viridae', 'virus'],
    'Bacteria': ['Bacteria', 'Proteobacteria', 'Firmicutes', 'Actinobacteria'],
    'Fungi': ['Fungi', 'Ascomycota', 'Basidiomycota', 'Mucoromycota', 'Microsporidia'],
    'Vectors': ['Diptera', 'Culicidae', 'Anopheles', 'Aedes', 'Culex', 'Glossina', 
                'Ixodida', 'Triatoma', 'Rhodnius', 'Phlebotomus', 'Lutzomyia'],
    'Hosts': ['Mammalia', 'Aves', 'Homo sapiens', 'Mus musculus', 'Gallus'],
    'Protists': ['Apicomplexa', 'Plasmodium', 'Trypanosoma', 'Leishmania', 
                 'Acanthamoeba', 'Giardia', 'Cryptosporidium', 'Toxoplasma',
                 'Babesia', 'Theileria', 'Entamoeba', 'Trichomonas', 'Naegleria'],
    'Helminths': ['Nematoda', 'Platyhelminthes', 'Schistosoma', 'Ascaris', 
                  'Brugia', 'Onchocerca', 'Wuchereria', 'Strongyloides',
                  'Trichuris', 'Ancylostoma', 'Necator', 'Fasciola', 'Taenia'],
}

# Color palette for charts
COLORS = {
    'Home': '#2563eb',
    'Organisms Index': '#7c3aed',
    'Assemblies Index': '#db2777',
    'Priority Pathogens Index': '#dc2626',
    'Roadmap': '#ea580c',
    'About': '#65a30d',
    'Calendar': '#0891b2',
    'Learn': '#6366f1',
    'Organism Pages': '#2563eb',
    'Assembly Pages': '#7c3aed',
    'Workflow Pages': '#db2777',
    'Priority Pathogens': '#dc2626',
    'Viruses': '#dc2626',
    'Bacteria': '#2563eb',
    'Fungi': '#65a30d',
    'Protists': '#7c3aed',
    'Vectors': '#ea580c',
    'Hosts': '#0891b2',
    'Helminths': '#db2777',
    'Other': '#6b7280',
}

_taxonomy_cache = {}
_assembly_taxonomy_cache = {}


def get_taxonomy_lineage(tax_id):
    """Fetch taxonomy lineage string from NCBI."""
    if tax_id in _taxonomy_cache:
        return _taxonomy_cache[tax_id]
    
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=taxonomy&id={tax_id}&retmode=xml"
    
    try:
        result = subprocess.run(
            ['curl', '-s', url],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout:
            _taxonomy_cache[tax_id] = ('Unknown', 'Unknown')
            return _taxonomy_cache[tax_id]
        
        lineage_match = re.search(r'<Lineage>([^<]+)</Lineage>', result.stdout)
        name_match = re.search(r'<ScientificName>([^<]+)</ScientificName>', result.stdout)
        
        lineage = lineage_match.group(1) if lineage_match else ''
        name = name_match.group(1) if name_match else 'Unknown'
        
        _taxonomy_cache[tax_id] = (name, lineage)
        return _taxonomy_cache[tax_id]
        
    except Exception:
        _taxonomy_cache[tax_id] = ('Unknown', 'Unknown')
        return _taxonomy_cache[tax_id]


def get_assembly_taxonomy(assembly_id):
    """Get taxonomy info for an assembly ID."""
    if assembly_id in _assembly_taxonomy_cache:
        return _assembly_taxonomy_cache[assembly_id]
    
    if '_' in assembly_id:
        parts = assembly_id.split('_')
        if len(parts) >= 3:
            accession = f"{parts[0]}_{parts[1]}"
        else:
            accession = assembly_id
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
            _assembly_taxonomy_cache[assembly_id] = (None, 'Unknown', 'Unknown')
            return _assembly_taxonomy_cache[assembly_id]
        
        data = json.loads(result.stdout)
        
        if 'reports' in data and len(data['reports']) > 0:
            report = data['reports'][0]
            if 'organism' in report:
                tax_id = report['organism'].get('tax_id')
                if tax_id:
                    name, lineage = get_taxonomy_lineage(tax_id)
                    _assembly_taxonomy_cache[assembly_id] = (tax_id, name, lineage)
                    return _assembly_taxonomy_cache[assembly_id]
        
        _assembly_taxonomy_cache[assembly_id] = (None, 'Unknown', 'Unknown')
        return _assembly_taxonomy_cache[assembly_id]
        
    except Exception:
        _assembly_taxonomy_cache[assembly_id] = (None, 'Unknown', 'Unknown')
        return _assembly_taxonomy_cache[assembly_id]


def classify_community(lineage):
    """Classify an organism into a community based on its lineage."""
    if not lineage or lineage == 'Unknown':
        return 'Other'
    
    lineage_lower = lineage.lower()
    
    for community, patterns in COMMUNITY_PATTERNS.items():
        for pattern in patterns:
            if pattern.lower() in lineage_lower:
                return community
    
    return 'Other'


def parse_data_file(filepath):
    """Parse a Plausible data file and extract page statistics."""
    stats = {
        'high_level': defaultdict(lambda: {'visitors': 0, 'pageviews': 0}),
        'organism_pages': [],
        'assembly_pages': [],
        'workflow_pages': [],
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
        next(f)
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
            
            if url in high_level_urls:
                name = high_level_urls[url]
                stats['high_level'][name]['visitors'] += visitors
                stats['high_level'][name]['pageviews'] += pageviews
            elif re.match(r'^/data/organisms/\d+$', url):
                tax_id = url.split('/')[-1]
                stats['organism_pages'].append((tax_id, visitors, pageviews))
            elif re.match(r'^/data/assemblies/[^/]+$', url):
                assembly_id = url.split('/')[-1]
                stats['assembly_pages'].append((assembly_id, visitors, pageviews))
            elif '/workflow-' in url:
                match = re.match(r'^/data/assemblies/([^/]+)/workflow-(.+)$', url)
                if match:
                    assembly_id = match.group(1)
                    workflow_name = match.group(2)
                    stats['workflow_pages'].append((assembly_id, workflow_name, visitors, pageviews))
            elif re.match(r'^/data/priority-pathogens/[^/]+$', url):
                pathogen = url.split('/')[-1]
                stats['priority_pathogen_pages'].append((pathogen, visitors, pageviews))
            elif url.startswith('/learn'):
                stats['learn_pages']['visitors'] += visitors
                stats['learn_pages']['pageviews'] += pageviews
    
    return stats


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


def generate_chart_js(chart_id, title, labels, datasets, y_label):
    """Generate Chart.js configuration for a line chart."""
    chart_data = {
        'type': 'line',
        'data': {
            'labels': labels,
            'datasets': datasets
        },
        'options': {
            'responsive': True,
            'maintainAspectRatio': False,
            'plugins': {
                'title': {
                    'display': True,
                    'text': title,
                    'font': {'size': 16, 'weight': 'bold'}
                },
                'legend': {
                    'position': 'bottom'
                }
            },
            'scales': {
                'y': {
                    'beginAtZero': True,
                    'title': {
                        'display': True,
                        'text': y_label
                    }
                },
                'x': {
                    'title': {
                        'display': True,
                        'text': 'Month'
                    }
                }
            },
            'interaction': {
                'intersect': False,
                'mode': 'index'
            }
        }
    }
    return json.dumps(chart_data)


def generate_html_report(monthly_data, output_path):
    """Generate the HTML report with charts."""
    
    months = [d['month'] for d in monthly_data]
    communities = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths', 'Other']
    
    # Prepare chart data
    charts = []
    
    # 1. High-level pages - Visitors
    high_level_pages = ['Home', 'Organisms Index', 'Assemblies Index', 'Priority Pathogens Index', 'Roadmap', 'About', 'Calendar']
    datasets = []
    for page in high_level_pages:
        data = [d['high_level'].get(page, {}).get('visitors', 0) for d in monthly_data]
        datasets.append({
            'label': page,
            'data': data,
            'borderColor': COLORS.get(page, '#6b7280'),
            'backgroundColor': COLORS.get(page, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('high_level_visitors', 'High-Level Pages - Visitors', datasets, 'Visitors'))
    
    # 2. High-level pages - Pageviews
    datasets = []
    for page in high_level_pages:
        data = [d['high_level'].get(page, {}).get('pageviews', 0) for d in monthly_data]
        datasets.append({
            'label': page,
            'data': data,
            'borderColor': COLORS.get(page, '#6b7280'),
            'backgroundColor': COLORS.get(page, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('high_level_pageviews', 'High-Level Pages - Pageviews', datasets, 'Pageviews'))
    
    # 3. Content pages - Unique pages
    content_types = ['Organism Pages', 'Assembly Pages', 'Workflow Pages', 'Priority Pathogens']
    datasets = []
    for ctype in content_types:
        key = ctype.lower().replace(' ', '_')
        if key == 'priority_pathogens':
            data = [d['priority_pathogens']['count'] for d in monthly_data]
        else:
            data = [d[key.replace('_pages', '_total')]['count'] for d in monthly_data]
        datasets.append({
            'label': ctype,
            'data': data,
            'borderColor': COLORS.get(ctype, '#6b7280'),
            'backgroundColor': COLORS.get(ctype, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('content_pages', 'Content Pages - Unique Pages Visited', datasets, 'Unique Pages'))
    
    # 4. Content pages - Visitors
    datasets = []
    for ctype in content_types:
        key = ctype.lower().replace(' ', '_')
        if key == 'priority_pathogens':
            data = [d['priority_pathogens']['visitors'] for d in monthly_data]
        else:
            data = [d[key.replace('_pages', '_total')]['visitors'] for d in monthly_data]
        datasets.append({
            'label': ctype,
            'data': data,
            'borderColor': COLORS.get(ctype, '#6b7280'),
            'backgroundColor': COLORS.get(ctype, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('content_visitors', 'Content Pages - Visitors', datasets, 'Visitors'))
    
    # 5. Content pages - Pageviews
    datasets = []
    for ctype in content_types:
        key = ctype.lower().replace(' ', '_')
        if key == 'priority_pathogens':
            data = [d['priority_pathogens']['pageviews'] for d in monthly_data]
        else:
            data = [d[key.replace('_pages', '_total')]['pageviews'] for d in monthly_data]
        datasets.append({
            'label': ctype,
            'data': data,
            'borderColor': COLORS.get(ctype, '#6b7280'),
            'backgroundColor': COLORS.get(ctype, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('content_pageviews', 'Content Pages - Pageviews', datasets, 'Pageviews'))
    
    # 6. Organism pages by community - Unique pages
    datasets = []
    for comm in communities:
        data = [d['organism_by_community'].get(comm, {}).get('count', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('organism_community_pages', 'Organism Pages by Community - Unique Pages', datasets, 'Unique Pages'))
    
    # 7. Organism pages by community - Visitors
    datasets = []
    for comm in communities:
        data = [d['organism_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('organism_community_visitors', 'Organism Pages by Community - Visitors', datasets, 'Visitors'))
    
    # 8. Assembly pages by community - Unique pages
    datasets = []
    for comm in communities:
        data = [d['assembly_by_community'].get(comm, {}).get('count', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('assembly_community_pages', 'Assembly Pages by Community - Unique Pages', datasets, 'Unique Pages'))
    
    # 9. Assembly pages by community - Visitors
    datasets = []
    for comm in communities:
        data = [d['assembly_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('assembly_community_visitors', 'Assembly Pages by Community - Visitors', datasets, 'Visitors'))
    
    # 10. Workflow pages by community - Unique pages
    datasets = []
    for comm in communities:
        data = [d['workflow_by_community'].get(comm, {}).get('count', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('workflow_community_pages', 'Workflow Pages by Community - Unique Pages', datasets, 'Unique Pages'))
    
    # 11. Workflow pages by community - Visitors
    datasets = []
    for comm in communities:
        data = [d['workflow_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data]
        datasets.append({
            'label': comm,
            'data': data,
            'borderColor': COLORS.get(comm, '#6b7280'),
            'backgroundColor': COLORS.get(comm, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('workflow_community_visitors', 'Workflow Pages by Community - Visitors', datasets, 'Visitors'))
    
    # 12. Learn pages
    datasets = [
        {
            'label': 'Visitors',
            'data': [d['learn']['visitors'] for d in monthly_data],
            'borderColor': COLORS['Learn'],
            'backgroundColor': COLORS['Learn'] + '20',
            'tension': 0.3,
            'fill': False
        },
        {
            'label': 'Pageviews',
            'data': [d['learn']['pageviews'] for d in monthly_data],
            'borderColor': '#a855f7',
            'backgroundColor': '#a855f720',
            'tension': 0.3,
            'fill': False
        }
    ]
    charts.append(('learn_pages', 'Learn / Featured Analyses Pages', datasets, 'Count'))
    
    # Generate HTML
    chart_containers = []
    chart_scripts = []
    
    for chart_id, title, datasets, y_label in charts:
        chart_containers.append(f'''
        <div class="chart-container">
            <canvas id="{chart_id}"></canvas>
        </div>
        ''')
        
        chart_config = generate_chart_js(chart_id, title, months, datasets, y_label)
        chart_scripts.append(f'''
        new Chart(document.getElementById('{chart_id}'), {chart_config});
        ''')
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BRC Analytics - Monthly Traffic Summary</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f8fafc;
            color: #1e293b;
        }}
        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding: 20px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        h1 {{
            margin: 0 0 10px 0;
            color: #0f172a;
        }}
        .subtitle {{
            color: #64748b;
            font-size: 14px;
        }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
            gap: 24px;
            max-width: 1800px;
            margin: 0 auto;
        }}
        .chart-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            height: 400px;
        }}
        .section-title {{
            font-size: 20px;
            font-weight: 600;
            margin: 40px 0 20px 0;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
            max-width: 1800px;
            margin-left: auto;
            margin-right: auto;
        }}
        .notes {{
            max-width: 1800px;
            margin: 40px auto;
            padding: 20px;
            background: #f1f5f9;
            border-radius: 12px;
            font-size: 14px;
            color: #475569;
        }}
        .notes h3 {{
            margin-top: 0;
            color: #334155;
        }}
        .notes ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .notes li {{
            margin-bottom: 8px;
        }}
        @media (max-width: 700px) {{
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            .chart-container {{
                height: 300px;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>BRC Analytics - Monthly Traffic Summary</h1>
        <div class="subtitle">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data range: {months[0]} - {months[-1]}</div>
    </div>
    
    <h2 class="section-title">High-Level Pages</h2>
    <div class="charts-grid">
        {chart_containers[0]}
        {chart_containers[1]}
    </div>
    
    <h2 class="section-title">Content Pages (Organism, Assembly, Workflow)</h2>
    <div class="charts-grid">
        {chart_containers[2]}
        {chart_containers[3]}
        {chart_containers[4]}
    </div>
    
    <h2 class="section-title">Organism Pages by Community</h2>
    <div class="charts-grid">
        {chart_containers[5]}
        {chart_containers[6]}
    </div>
    
    <h2 class="section-title">Assembly Pages by Community</h2>
    <div class="charts-grid">
        {chart_containers[7]}
        {chart_containers[8]}
    </div>
    
    <h2 class="section-title">Workflow Pages by Community</h2>
    <div class="charts-grid">
        {chart_containers[9]}
        {chart_containers[10]}
    </div>
    
    <h2 class="section-title">Learn / Featured Analyses</h2>
    <div class="charts-grid">
        {chart_containers[11]}
    </div>
    
    <div class="notes">
        <h3>Notes</h3>
        <ul>
            <li><strong>Organism Pages</strong> = /data/organisms/{{tax_id}} (individual organism detail pages)</li>
            <li><strong>Assembly Pages</strong> = /data/assemblies/{{assembly_id}} (individual assembly detail pages)</li>
            <li><strong>Workflow Pages</strong> = /data/assemblies/{{id}}/workflow-{{...}} (workflow configuration pages)</li>
            <li><strong>Index pages</strong> (Organisms Index, etc.) are navigation/listing pages, not detail pages</li>
            <li><strong>Community classification</strong> is based on NCBI taxonomy lineage</li>
            <li><strong>Unique Pages</strong> = number of distinct URLs visited that month</li>
            <li><strong>Visitors</strong> = unique visitors to those pages</li>
            <li><strong>Pageviews</strong> = total page loads (includes repeat visits)</li>
        </ul>
    </div>
    
    <script>
        {''.join(chart_scripts)}
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w') as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser(description="Generate HTML monthly summary report with charts")
    parser.add_argument('--output', '-o', default='output/monthly_summary.html', help="Output HTML file")
    parser.add_argument('--no-cache', action='store_true', help="Don't use taxonomy cache")
    parser.add_argument('--verbose', '-v', action='store_true', help="Show detailed progress")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data' / 'fetched'
    cache_file = script_dir.parent / '.taxonomy_cache.json'
    output_path = Path(args.output)
    
    if not output_path.is_absolute():
        output_path = script_dir.parent / output_path
    
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
    
    # Collect all unique IDs
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
    
    # Pre-fetch taxonomy data
    uncached_tax_ids = [t for t in all_tax_ids if t not in _taxonomy_cache]
    if uncached_tax_ids:
        print(f"Fetching taxonomy for {len(uncached_tax_ids)} tax IDs...", file=sys.stderr)
        for i, tax_id in enumerate(uncached_tax_ids, 1):
            if args.verbose:
                print(f"  [{i}/{len(uncached_tax_ids)}] Tax ID {tax_id}", file=sys.stderr)
            get_taxonomy_lineage(tax_id)
            time.sleep(0.35)
    
    uncached_assemblies = [a for a in all_assembly_ids if a not in _assembly_taxonomy_cache]
    if uncached_assemblies:
        print(f"Fetching taxonomy for {len(uncached_assemblies)} assemblies...", file=sys.stderr)
        for i, assembly_id in enumerate(uncached_assemblies, 1):
            if args.verbose:
                print(f"  [{i}/{len(uncached_assemblies)}] Assembly {assembly_id}", file=sys.stderr)
            get_assembly_taxonomy(assembly_id)
            time.sleep(0.35)
    
    # Save cache
    if not args.no_cache:
        save_taxonomy_cache(cache_file)
    
    # Process each month
    monthly_data = []
    
    print("Processing monthly data...", file=sys.stderr)
    for year, month, filepath in month_files:
        month_label = format_month(year, month)
        print(f"  Processing {month_label}...", file=sys.stderr)
        
        stats = parse_data_file(filepath)
        
        # Aggregate by community
        org_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for tax_id, visitors, pageviews in stats['organism_pages']:
            name, lineage = _taxonomy_cache.get(tax_id, ('Unknown', 'Unknown'))
            community = classify_community(lineage)
            org_by_community[community]['count'] += 1
            org_by_community[community]['visitors'] += visitors
            org_by_community[community]['pageviews'] += pageviews
        
        asm_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for assembly_id, visitors, pageviews in stats['assembly_pages']:
            _, name, lineage = _assembly_taxonomy_cache.get(assembly_id, (None, 'Unknown', 'Unknown'))
            community = classify_community(lineage)
            asm_by_community[community]['count'] += 1
            asm_by_community[community]['visitors'] += visitors
            asm_by_community[community]['pageviews'] += pageviews
        
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
    
    # Generate HTML report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate_html_report(monthly_data, output_path)
    
    print(f"\nHTML report saved to: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
