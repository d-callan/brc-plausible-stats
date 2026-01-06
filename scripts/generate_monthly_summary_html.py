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
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Import shared taxonomy module
from taxonomy_cache import load_cache, get_community

# Workflow category patterns for classification
WORKFLOW_CATEGORIES = {
    'Variant Calling': ['variant-calling', 'haploid-variant'],
    'Transcription': ['rnaseq', 'lncRNAs', 'transcriptome'],
    'Single Cell': ['scrna-seq', '10x-', 'cellplex', 'single-cell'],
    'Epigenomics': ['chipseq', 'atacseq', 'cutandrun', 'consensus-peaks'],
    'AMR': ['amr-gene', 'antimicrobial'],
    'Viral': ['viral', 'sars-cov', 'covid'],
}

WORKFLOW_CATEGORIES_ORDER = ['Variant Calling', 'Transcription', 'Single Cell', 'Epigenomics', 'AMR', 'Viral', 'Other']

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
    # Workflow categories
    'Variant Calling': '#dc2626',
    'Transcription': '#2563eb',
    'Single Cell': '#7c3aed',
    'Epigenomics': '#65a30d',
    'AMR': '#ea580c',
    'Viral': '#0891b2',
}

# Load taxonomy cache once at module level
_taxonomy_cache = {}
_assembly_cache = {}


def load_taxonomy_caches():
    """Load taxonomy caches if not already loaded."""
    global _taxonomy_cache, _assembly_cache
    if not _taxonomy_cache:
        _taxonomy_cache, _assembly_cache = load_cache()


def classify_community(lineage):
    """Classify an organism into a community based on its lineage."""
    return get_community(lineage)


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


def parse_demographics_file(filepath):
    """Parse a demographics TSV file."""
    data = {}
    if not filepath.exists():
        return data
        
    try:
        with open(filepath, 'r') as f:
            # Skip header
            header = next(f, None)
            if not header:
                return data
                
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    key = parts[0]
                    try:
                        visitors = int(parts[1])
                        data[key] = visitors
                    except ValueError:
                        continue
    except Exception:
        pass
    return data


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


def generate_bar_chart_js(chart_id, title, labels, datasets, y_label):
    """Generate Chart.js configuration for a grouped bar chart."""
    chart_data = {
        'type': 'bar',
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
                        'text': 'Community'
                    }
                }
            }
        }
    }
    return json.dumps(chart_data)


def generate_html_report(monthly_data, output_path, all_time_data=None):
    """Generate the HTML report with charts.
    
    Args:
        monthly_data: List of monthly data dicts
        output_path: Path to write HTML file
        all_time_data: Optional dict with all-time community stats (from dedicated fetch)
    """
    
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
    
    # 12. Workflow pages by category - Unique pages
    datasets = []
    for cat in WORKFLOW_CATEGORIES_ORDER:
        data = [d['workflow_by_category'].get(cat, {}).get('count', 0) for d in monthly_data]
        datasets.append({
            'label': cat,
            'data': data,
            'borderColor': COLORS.get(cat, '#6b7280'),
            'backgroundColor': COLORS.get(cat, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('workflow_category_pages', 'Workflow Pages by Category - Unique Pages', datasets, 'Unique Pages'))
    
    # 13. Workflow pages by category - Visitors
    datasets = []
    for cat in WORKFLOW_CATEGORIES_ORDER:
        data = [d['workflow_by_category'].get(cat, {}).get('visitors', 0) for d in monthly_data]
        datasets.append({
            'label': cat,
            'data': data,
            'borderColor': COLORS.get(cat, '#6b7280'),
            'backgroundColor': COLORS.get(cat, '#6b7280') + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('workflow_category_visitors', 'Workflow Pages by Category - Visitors', datasets, 'Visitors'))
    
    # 14. Learn pages
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
    
    # --- Demographics Charts ---
    
    # Helper to get top keys across all months
    def get_top_keys(category, limit=5):
        totals = defaultdict(int)
        for d in monthly_data:
            data = d.get('demographics', {}).get(category, {})
            for k, v in data.items():
                totals[k] += v
        return sorted(totals.keys(), key=lambda k: totals[k], reverse=True)[:limit]

    # 15. Top Countries
    top_countries = get_top_keys('countries', 8)
    datasets = []
    # Palette for demographics (cycling colors)
    demo_colors = ['#2563eb', '#7c3aed', '#db2777', '#dc2626', '#ea580c', '#65a30d', '#0891b2', '#6366f1', '#4b5563']
    
    for i, country in enumerate(top_countries):
        data = [d.get('demographics', {}).get('countries', {}).get(country, 0) for d in monthly_data]
        color = demo_colors[i % len(demo_colors)]
        datasets.append({
            'label': country,
            'data': data,
            'borderColor': color,
            'backgroundColor': color + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('demo_countries', 'Top Countries - Visitors', datasets, 'Visitors'))

    # 16. Devices
    top_devices = get_top_keys('devices', 5)
    datasets = []
    for i, device in enumerate(top_devices):
        data = [d.get('demographics', {}).get('devices', {}).get(device, 0) for d in monthly_data]
        color = demo_colors[i % len(demo_colors)]
        datasets.append({
            'label': device,
            'data': data,
            'borderColor': color,
            'backgroundColor': color + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('demo_devices', 'Devices - Visitors', datasets, 'Visitors'))
    
    # 17. Browsers
    top_browsers = get_top_keys('browsers', 6)
    datasets = []
    for i, browser in enumerate(top_browsers):
        data = [d.get('demographics', {}).get('browsers', {}).get(browser, 0) for d in monthly_data]
        color = demo_colors[i % len(demo_colors)]
        datasets.append({
            'label': browser,
            'data': data,
            'borderColor': color,
            'backgroundColor': color + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('demo_browsers', 'Top Browsers - Visitors', datasets, 'Visitors'))
    
    # 18. Sources
    top_sources = get_top_keys('sources', 8)
    datasets = []
    for i, source in enumerate(top_sources):
        data = [d.get('demographics', {}).get('sources', {}).get(source, 0) for d in monthly_data]
        color = demo_colors[i % len(demo_colors)]
        datasets.append({
            'label': source,
            'data': data,
            'borderColor': color,
            'backgroundColor': color + '20',
            'tension': 0.3,
            'fill': False
        })
    charts.append(('demo_sources', 'Traffic Sources - Visitors', datasets, 'Visitors'))
    
    # Per-community bar charts showing organism/assembly/workflow relationships
    # Use all_time_data if available (more accurate), otherwise aggregate from monthly
    if all_time_data:
        community_totals = all_time_data
        bar_chart_note = "(from dedicated all-time fetch)"
    else:
        # Fallback: aggregate from monthly data (may overcount unique visitors)
        community_totals = {}
        for comm in communities:
            community_totals[comm] = {
                'organism_pages': sum(d['organism_by_community'].get(comm, {}).get('count', 0) for d in monthly_data),
                'organism_visitors': sum(d['organism_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data),
                'assembly_pages': sum(d['assembly_by_community'].get(comm, {}).get('count', 0) for d in monthly_data),
                'assembly_visitors': sum(d['assembly_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data),
                'workflow_pages': sum(d['workflow_by_community'].get(comm, {}).get('count', 0) for d in monthly_data),
                'workflow_visitors': sum(d['workflow_by_community'].get(comm, {}).get('visitors', 0) for d in monthly_data),
            }
        bar_chart_note = "(aggregated from monthly - may overcount)"
    
    # Bar chart colors for page types
    bar_colors = {
        'Organism Pages': '#2563eb',
        'Assembly Pages': '#7c3aed', 
        'Workflow Pages': '#db2777',
    }
    
    # Community comparison - Unique Pages (all time totals)
    bar_charts = []
    datasets = [
        {
            'label': 'Organism Pages',
            'data': [community_totals[c]['organism_pages'] for c in communities],
            'backgroundColor': bar_colors['Organism Pages'],
        },
        {
            'label': 'Assembly Pages',
            'data': [community_totals[c]['assembly_pages'] for c in communities],
            'backgroundColor': bar_colors['Assembly Pages'],
        },
        {
            'label': 'Workflow Pages',
            'data': [community_totals[c]['workflow_pages'] for c in communities],
            'backgroundColor': bar_colors['Workflow Pages'],
        },
    ]
    community_pages_bar_title = f"Page Types by Community - Unique Pages {bar_chart_note}"
    bar_charts.append(('community_pages_bar', community_pages_bar_title, communities, datasets, 'Unique Pages'))
    
    # Community comparison - Visitors (all time totals)
    datasets = [
        {
            'label': 'Organism Pages',
            'data': [community_totals[c]['organism_visitors'] for c in communities],
            'backgroundColor': bar_colors['Organism Pages'],
        },
        {
            'label': 'Assembly Pages',
            'data': [community_totals[c]['assembly_visitors'] for c in communities],
            'backgroundColor': bar_colors['Assembly Pages'],
        },
        {
            'label': 'Workflow Pages',
            'data': [community_totals[c]['workflow_visitors'] for c in communities],
            'backgroundColor': bar_colors['Workflow Pages'],
        },
    ]
    bar_charts.append(('community_visitors_bar', f'Page Types by Community - Visitors {bar_chart_note}', communities, datasets, 'Visitors'))
    
    # Generate HTML
    chart_containers = []
    chart_scripts = []
    
    clickable_chart_ids = {
        'organism_community_pages',
        'organism_community_visitors',
        'assembly_community_pages',
        'assembly_community_visitors',
        'workflow_community_pages',
        'workflow_community_visitors',
        'workflow_category_pages',
        'workflow_category_visitors',
    }

    for chart_id, title, datasets, y_label in charts:
        is_clickable = chart_id in clickable_chart_ids
        clickable_class = 'clickable' if is_clickable else ''
        indicator = '<div class="clickable-indicator">Click for details</div>' if is_clickable else ''
        chart_containers.append(f'''
        <div class="chart-container {clickable_class}">
            {indicator}
            <canvas id="{chart_id}"></canvas>
        </div>
        ''')
        
        chart_config = generate_chart_js(chart_id, title, months, datasets, y_label)
        chart_scripts.append(f'''
        new Chart(document.getElementById('{chart_id}'), {chart_config});
        ''')
    
    # Generate bar chart containers and scripts
    bar_chart_containers = []
    for chart_id, title, labels, datasets, y_label in bar_charts:
        bar_chart_containers.append(f'''
        <div class="chart-container">
            <canvas id="{chart_id}"></canvas>
        </div>
        ''')
        
        chart_config = generate_bar_chart_js(chart_id, title, labels, datasets, y_label)
        chart_scripts.append(f'''
        new Chart(document.getElementById('{chart_id}'), {chart_config});
        ''')
    
    # Generate network section if all_time_data has network info
    network_section = ''
    if all_time_data and '_network' in all_time_data:
        network_data = all_time_data['_network']
        network_json = json.dumps(network_data)
        network_section = f'''
    <h2 class="section-title">Workflow Categories by Organism Community (All-Time)</h2>
    <div class="network-container">
        <div class="network-legend">
            <span><span class="dot" style="background:#db2777;"></span>Workflow Category</span>
            <span><span class="dot" style="background:#2563eb;"></span>Organism Community</span>
            <span style="margin-left:20px;color:#64748b;font-style:italic;">Node size = visitors | Edge width = visitor connections</span>
        </div>
        <svg id="networkSvg"></svg>
    </div>
    <script>
        (function() {{
            const networkData = {network_json};
            
            if (networkData.workflows.length > 0 && networkData.communities.length > 0) {{
                const svg = d3.select('#networkSvg');
                const container = document.querySelector('.network-container');
                const width = container.clientWidth - 40;
                const height = container.clientHeight - 60;
                
                svg.attr('width', width).attr('height', height);
                
                const g = svg.append('g');
                
                const zoom = d3.zoom()
                    .scaleExtent([0.2, 3])
                    .on('zoom', (event) => g.attr('transform', event.transform));
                svg.call(zoom);
                
                const nodes = [
                    ...networkData.workflows.map(w => ({{ id: w.id, type: 'workflow', visitors: w.visitors }})),
                    ...networkData.communities.map(c => ({{ id: c.id, type: 'community', visitors: c.visitors }}))
                ];
                
                const links = networkData.edges.map(e => ({{
                    source: e.source,
                    target: e.target,
                    visitors: e.visitors
                }}));
                
                const maxVisitors = Math.max(...nodes.map(n => n.visitors));
                const nodeScale = d3.scaleSqrt().domain([1, maxVisitors]).range([6, 20]);
                
                const maxEdgeVisitors = Math.max(...links.map(l => l.visitors));
                const edgeScale = d3.scaleLinear().domain([1, maxEdgeVisitors]).range([1, 6]);
                
                const simulation = d3.forceSimulation(nodes)
                    .force('link', d3.forceLink(links).id(d => d.id).distance(80))
                    .force('charge', d3.forceManyBody().strength(-100))
                    .force('center', d3.forceCenter(width / 2, height / 2))
                    .force('collision', d3.forceCollide().radius(d => nodeScale(d.visitors) + 4))
                    .force('x', d3.forceX(width / 2).strength(0.03))
                    .force('y', d3.forceY(height / 2).strength(0.03));
                
                const link = g.append('g')
                    .selectAll('line')
                    .data(links)
                    .join('line')
                    .attr('class', 'link')
                    .attr('stroke-width', d => edgeScale(d.visitors));
                
                const node = g.append('g')
                    .selectAll('circle')
                    .data(nodes)
                    .join('circle')
                    .attr('r', d => nodeScale(d.visitors))
                    .attr('class', d => d.type === 'workflow' ? 'node-workflow' : 'node-organism')
                    .call(d3.drag()
                        .on('start', dragstarted)
                        .on('drag', dragged)
                        .on('end', dragended));
                
                const label = g.append('g')
                    .selectAll('text')
                    .data(nodes)
                    .join('text')
                    .attr('class', 'node-label')
                    .attr('dy', d => nodeScale(d.visitors) + 10)
                    .attr('text-anchor', 'middle')
                    .text(d => d.id.length > 18 ? d.id.slice(0, 18) + '...' : d.id);
                
                node.append('title')
                    .text(d => d.id + '\\n' + d.visitors + ' visitors');
                
                simulation.on('tick', () => {{
                    link
                        .attr('x1', d => d.source.x)
                        .attr('y1', d => d.source.y)
                        .attr('x2', d => d.target.x)
                        .attr('y2', d => d.target.y);
                    
                    node
                        .attr('cx', d => d.x)
                        .attr('cy', d => d.y);
                    
                    label
                        .attr('x', d => d.x)
                        .attr('y', d => d.y);
                }});
                
                simulation.on('end', () => {{
                    const bounds = g.node().getBBox();
                    const scale = 0.85 / Math.max(bounds.width / width, bounds.height / height);
                    const tx = (width - scale * (bounds.x * 2 + bounds.width)) / 2;
                    const ty = (height - scale * (bounds.y * 2 + bounds.height)) / 2;
                    svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
                }});
                
                function dragstarted(event) {{
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    event.subject.fx = event.subject.x;
                    event.subject.fy = event.subject.y;
                }}
                
                function dragged(event) {{
                    event.subject.fx = event.x;
                    event.subject.fy = event.y;
                }}
                
                function dragended(event) {{
                    if (!event.active) simulation.alphaTarget(0);
                    event.subject.fx = null;
                    event.subject.fy = null;
                }}
            }}
        }})();
    </script>
        '''
    
    # Generate month-to-report URL mapping
    # Format: {"Oct 2024": {"organism": "fetched/top-pages-2024-10-01-to-2024-10-31-organism-analysis.html", "workflow": "..."}}
    month_reports = {}
    for d in monthly_data:
        month_label = d['month']
        year = d['year']
        month_num = d['month_num']
        # Calculate last day of month
        if month_num == 12:
            last_day = 31
        elif month_num in [4, 6, 9, 11]:
            last_day = 30
        elif month_num == 2:
            last_day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
        else:
            last_day = 31
        
        date_range = f"{year}-{month_num:02d}-01-to-{year}-{month_num:02d}-{last_day:02d}"
        month_reports[month_label] = {
            'organism': f"fetched/top-pages-{date_range}-organism-analysis.html",
            'workflow': f"fetched/top-pages-{date_range}-workflow-analysis.html"
        }
    month_reports_json = json.dumps(month_reports)
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BRC Analytics - Monthly Traffic Summary</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://d3js.org/d3.v7.min.js"></script>
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
            position: relative;
        }}
        .chart-container.clickable {{
            cursor: default;
            transition: none;
        }}
        .chart-container.clickable canvas {{
            cursor: pointer;
        }}
        .chart-container.clickable canvas:hover {{
            outline: 2px solid #bfdbfe;
            outline-offset: 2px;
        }}
        .clickable-indicator {{
            position: absolute;
            top: 10px;
            right: 10px;
            background: #eff6ff;
            color: #1d4ed8;
            border: 1px solid #bfdbfe;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 500;
            opacity: 0.95;
            pointer-events: none;
        }}
        .click-hint {{
            margin-top: 12px;
            display: inline-block;
            background: #eff6ff;
            color: #1d4ed8;
            border: 1px solid #bfdbfe;
            padding: 8px 12px;
            border-radius: 999px;
            font-size: 13px;
            font-weight: 500;
        }}
        .click-hint strong {{
            font-weight: 700;
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
        .network-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            height: 600px;
            max-width: 1800px;
            margin: 0 auto 24px auto;
            position: relative;
        }}
        #networkSvg {{ width: 100%; height: 100%; }}
        .node-workflow {{ fill: #db2777; }}
        .node-organism {{ fill: #2563eb; }}
        .node-label {{ font-size: 9px; fill: #1e293b; pointer-events: none; }}
        .link {{ stroke: #94a3b8; stroke-opacity: 0.6; }}
        .network-legend {{
            position: absolute;
            top: 10px;
            right: 20px;
            font-size: 12px;
            background: rgba(255,255,255,0.9);
            padding: 8px 12px;
            border-radius: 6px;
        }}
        .network-legend span {{ display: inline-flex; align-items: center; margin-left: 12px; }}
        .network-legend .dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; }}
        @media (max-width: 700px) {{
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            .chart-container {{
                height: 300px;
            }}
            .network-container {{
                height: 400px;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>BRC Analytics - Monthly Traffic Summary</h1>
        <div class="subtitle">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data range: {months[0]} - {months[-1]}</div>
        <div class="click-hint"><strong>Tip:</strong> charts labeled “Click for details” are interactive and open a drill-down report.</div>
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
    
    <h2 class="section-title">Community Comparison - Page Type Breakdown</h2>
    <div class="charts-grid">
        {bar_chart_containers[0]}
        {bar_chart_containers[1]}
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
    
    <h2 class="section-title">Workflow Pages by Category</h2>
    <div class="charts-grid">
        {chart_containers[11]}
        {chart_containers[12]}
    </div>
    
    {network_section}
    
    <h2 class="section-title">Learn / Featured Analyses</h2>
    <div class="charts-grid">
        {chart_containers[13]}
    </div>
    
    <h2 class="section-title">Demographics & Technology</h2>
    <div class="charts-grid">
        {chart_containers[14]}
        {chart_containers[15]}
        {chart_containers[16]}
        {chart_containers[17]}
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
            <li><strong>Click on chart data points</strong> to view detailed monthly reports (organism/workflow analysis)</li>
        </ul>
    </div>
    
    <script>
        // Month label to report URL mapping
        const monthReports = {month_reports_json};
        
        // Add click handlers to charts for navigation to monthly reports
        function addChartClickHandler(chartId, reportType) {{
            const chart = Chart.getChart(chartId);
            if (!chart) return;
            
            chart.options.onClick = function(event, elements) {{
                if (elements.length > 0) {{
                    const index = elements[0].index;
                    const monthLabel = chart.data.labels[index];
                    const report = monthReports[monthLabel];
                    if (report && report[reportType]) {{
                        window.location.href = report[reportType];
                    }}
                }}
            }};
            chart.options.onHover = function(event, elements) {{
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }};
            chart.update();
        }}
        
        {''.join(chart_scripts)}
        
        // Add click handlers to organism and workflow charts
        // Charts 5-6: Organism by community -> organism analysis
        addChartClickHandler('organism_community_pages', 'organism');
        addChartClickHandler('organism_community_visitors', 'organism');
        // Charts 7-8: Assembly by community -> organism analysis (assemblies are in organism report)
        addChartClickHandler('assembly_community_pages', 'organism');
        addChartClickHandler('assembly_community_visitors', 'organism');
        // Charts 9-10: Workflow by community -> workflow analysis
        addChartClickHandler('workflow_community_pages', 'workflow');
        addChartClickHandler('workflow_community_visitors', 'workflow');
        // Charts 11-12: Workflow by category -> workflow analysis
        addChartClickHandler('workflow_category_pages', 'workflow');
        addChartClickHandler('workflow_category_visitors', 'workflow');
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
    output_path = Path(args.output)
    
    if not output_path.is_absolute():
        output_path = script_dir.parent / output_path
    
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Load taxonomy cache
    print("Loading taxonomy cache...", file=sys.stderr)
    load_taxonomy_caches()
    print(f"  Loaded {len(_taxonomy_cache)} taxonomy entries", file=sys.stderr)
    print(f"  Loaded {len(_assembly_cache)} assembly entries", file=sys.stderr)
    
    # Get all monthly files
    month_files = get_month_files(data_dir)
    if not month_files:
        print("Error: No monthly data files found", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(month_files)} monthly data files", file=sys.stderr)
    
    # Process each month
    monthly_data = []
    
    print("Processing monthly data...", file=sys.stderr)
    for year, month, filepath in month_files:
        month_label = format_month(year, month)
        print(f"  Processing {month_label}...", file=sys.stderr)
        
        stats = parse_data_file(filepath)
        
        # Load demographics data
        # Construct filename pattern based on date range in filename
        # filepath is like top-pages-2024-10-01-to-2024-10-31.tab
        date_range_part = filepath.name.replace('top-pages-', '').replace('.tab', '')
        
        demo_data = {}
        for demo_type in ['countries', 'devices', 'browsers', 'sources']:
            demo_file = data_dir / f"demographics-{demo_type}-{date_range_part}.tab"
            demo_data[demo_type] = parse_demographics_file(demo_file)

        # Aggregate by community
        org_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for tax_id, visitors, pageviews in stats['organism_pages']:
            tax_data = _taxonomy_cache.get(tax_id, {})
            lineage = tax_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            org_by_community[community]['count'] += 1
            org_by_community[community]['visitors'] += visitors
            org_by_community[community]['pageviews'] += pageviews
        
        asm_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for assembly_id, visitors, pageviews in stats['assembly_pages']:
            asm_data = _assembly_cache.get(assembly_id, {})
            lineage = asm_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            asm_by_community[community]['count'] += 1
            asm_by_community[community]['visitors'] += visitors
            asm_by_community[community]['pageviews'] += pageviews
        
        wf_by_community = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        wf_by_category = defaultdict(lambda: {'count': 0, 'visitors': 0, 'pageviews': 0})
        for assembly_id, workflow, visitors, pageviews in stats['workflow_pages']:
            asm_data = _assembly_cache.get(assembly_id, {})
            lineage = asm_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            wf_by_community[community]['count'] += 1
            wf_by_community[community]['visitors'] += visitors
            wf_by_community[community]['pageviews'] += pageviews
            # Also classify by workflow category
            category = classify_workflow_category(workflow)
            wf_by_category[category]['count'] += 1
            wf_by_category[category]['visitors'] += visitors
            wf_by_category[category]['pageviews'] += pageviews
        
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
            'workflow_by_category': dict(wf_by_category),
            'priority_pathogens': {
                'count': len(stats['priority_pathogen_pages']),
                'visitors': sum(v for _, v, _ in stats['priority_pathogen_pages']),
                'pageviews': sum(p for _, _, p in stats['priority_pathogen_pages']),
            },
            'learn': stats['learn_pages'],
            'demographics': demo_data,
        })
    
    # Check for all-time data file and process it
    all_time_file = data_dir / 'top-pages-all-time.tab'
    all_time_data = None
    
    if all_time_file.exists():
        print("Processing all-time data for bar charts...", file=sys.stderr)
        all_time_stats = parse_data_file(all_time_file)
        
        communities = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths', 'Other']
        all_time_data = {comm: {
            'organism_pages': 0, 'organism_visitors': 0,
            'assembly_pages': 0, 'assembly_visitors': 0,
            'workflow_pages': 0, 'workflow_visitors': 0,
        } for comm in communities}
        
        # Process organism pages
        for tax_id, visitors, pageviews in all_time_stats['organism_pages']:
            tax_data = _taxonomy_cache.get(tax_id, {})
            lineage = tax_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            all_time_data[community]['organism_pages'] += 1
            all_time_data[community]['organism_visitors'] += visitors
        
        # Process assembly pages
        for assembly_id, visitors, pageviews in all_time_stats['assembly_pages']:
            asm_data = _assembly_cache.get(assembly_id, {})
            lineage = asm_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            all_time_data[community]['assembly_pages'] += 1
            all_time_data[community]['assembly_visitors'] += visitors
        
        # Process workflow pages and build network data (workflow categories <-> organism communities)
        workflow_nodes = {}
        community_nodes = {}
        network_edges = {}  # Use dict for easy aggregation
        
        for assembly_id, workflow, visitors, pageviews in all_time_stats['workflow_pages']:
            asm_data = _assembly_cache.get(assembly_id, {})
            lineage = asm_data.get('lineage', 'Unknown')
            community = classify_community(lineage)
            all_time_data[community]['workflow_pages'] += 1
            all_time_data[community]['workflow_visitors'] += visitors
            
            # Build network data - workflow category to organism community
            wf_category = classify_workflow_category(workflow)
            
            if wf_category not in workflow_nodes:
                workflow_nodes[wf_category] = {'visitors': 0}
            workflow_nodes[wf_category]['visitors'] += visitors
            
            if community not in community_nodes:
                community_nodes[community] = {'visitors': 0}
            community_nodes[community]['visitors'] += visitors
            
            # Aggregate edges by workflow category - community pair
            edge_key = (wf_category, community)
            if edge_key not in network_edges:
                network_edges[edge_key] = {'source': wf_category, 'target': community, 'visitors': 0}
            network_edges[edge_key]['visitors'] += visitors
        
        # Store network data
        all_time_data['_network'] = {
            'workflows': [{'id': k, 'visitors': v['visitors']} for k, v in workflow_nodes.items()],
            'communities': [{'id': k, 'visitors': v['visitors']} for k, v in community_nodes.items()],
            'edges': list(network_edges.values())
        }
        
        # Load all-time demographics
        # The filename depends on the date range used during fetch, which is dynamic (launch to today).
        # We need to find the file that starts with demographics-countries- and has "2024-10-01" as start.
        # Since we might not know the exact end date used in fetch, we'll search for it.
        all_time_demo = {}
        for demo_type in ['countries', 'devices', 'browsers', 'sources']:
            # Find file pattern: demographics-{demo_type}-2024-10-01-to-*.tab
            found_files = list(data_dir.glob(f"demographics-{demo_type}-2024-10-01-to-*.tab"))
            if found_files:
                # Pick the most recent one (modification time) or just the first one
                # There should ideally be only one if we clean up, but let's take the one with latest end date
                latest_file = max(found_files, key=lambda f: f.stat().st_mtime)
                all_time_demo[demo_type] = parse_demographics_file(latest_file)
            else:
                all_time_demo[demo_type] = {}
        
        all_time_data['demographics'] = all_time_demo
        
    else:
        print("No all-time data file found, bar charts will use aggregated monthly data", file=sys.stderr)
        print("  (Run: python3 scripts/fetch_monthly_reports.py --include-all-time)", file=sys.stderr)
    
    # Generate HTML report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate_html_report(monthly_data, output_path, all_time_data)
    
    print(f"\nHTML report saved to: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
