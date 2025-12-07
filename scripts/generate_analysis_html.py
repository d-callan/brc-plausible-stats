#!/usr/bin/env python3
"""
Generate HTML reports from monthly analysis text files.

This script converts the text-based organism and workflow analysis reports
into interactive HTML pages with tables and charts.

Usage:
    python3 generate_analysis_html.py output/fetched/top-pages-2025-05-01-to-2025-05-31-organism-analysis.txt
    python3 generate_analysis_html.py output/fetched/  # Process all analysis files in directory
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

# Community classification patterns (same as monthly summary)
COMMUNITY_PATTERNS = {
    'Viruses': ['Viruses', 'Viridae', 'virus', 'Monkeypox', 'Influenza', 'Variola', 'Orthopoxvirus'],
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

COMMUNITY_COLORS = {
    'Viruses': '#dc2626',
    'Bacteria': '#2563eb',
    'Fungi': '#65a30d',
    'Protists': '#7c3aed',
    'Vectors': '#ea580c',
    'Hosts': '#0891b2',
    'Helminths': '#db2777',
    'Other': '#6b7280',
}

COMMUNITIES_ORDER = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths', 'Other']

_taxonomy_cache = {}
_assembly_taxonomy_cache = {}


def load_taxonomy_cache(cache_file):
    """Load taxonomy cache from file."""
    global _taxonomy_cache, _assembly_taxonomy_cache
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            data = json.load(f)
            _taxonomy_cache = {k: tuple(v) for k, v in data.get('taxonomy', {}).items()}
            _assembly_taxonomy_cache = {k: tuple(v) for k, v in data.get('assembly', {}).items()}


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
            timeout=30
        )
        
        xml_content = result.stdout
        
        name_match = re.search(r'<ScientificName>([^<]+)</ScientificName>', xml_content)
        name = name_match.group(1) if name_match else 'Unknown'
        
        lineage_match = re.search(r'<Lineage>([^<]+)</Lineage>', xml_content)
        lineage = lineage_match.group(1) if lineage_match else 'Unknown'
        
        _taxonomy_cache[tax_id] = (name, lineage)
        return (name, lineage)
    except Exception:
        _taxonomy_cache[tax_id] = ('Unknown', 'Unknown')
        return ('Unknown', 'Unknown')


def get_assembly_taxonomy(assembly_id):
    """Get taxonomy info for an assembly from NCBI."""
    if assembly_id in _assembly_taxonomy_cache:
        return _assembly_taxonomy_cache[assembly_id]
    
    clean_id = assembly_id.replace('_', '.')
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/{clean_id}/dataset_report"
    
    try:
        result = subprocess.run(
            ['curl', '-s', '-H', 'Accept: application/json', url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        data = json.loads(result.stdout)
        reports = data.get('reports', [])
        if reports:
            org_info = reports[0].get('organism', {})
            tax_id = str(org_info.get('tax_id', ''))
            name = org_info.get('organism_name', 'Unknown')
            
            if tax_id and tax_id in _taxonomy_cache:
                _, lineage = _taxonomy_cache[tax_id]
            elif tax_id:
                _, lineage = get_taxonomy_lineage(tax_id)
            else:
                lineage = 'Unknown'
            
            _assembly_taxonomy_cache[assembly_id] = (tax_id, name, lineage)
            return (tax_id, name, lineage)
    except Exception:
        pass
    
    _assembly_taxonomy_cache[assembly_id] = (None, 'Unknown', 'Unknown')
    return (None, 'Unknown', 'Unknown')


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


def parse_organism_analysis(filepath):
    """Parse an organism analysis text file."""
    data = {
        'title': 'Organism and Pathogen Page Analysis',
        'date_range': '',
        'overall_stats': {},
        'high_level_pages': [],
        'priority_pathogens': [],
        'organism_pages_all': [],
        'organism_pages_no_assembly': [],
        'assembly_pages_all': [],
        'assembly_pages_no_workflow': [],
    }
    
    # Extract date range from filename
    match = re.search(r'(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})', filepath.name)
    if match:
        data['date_range'] = f"{match.group(1)} to {match.group(2)}"
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Parse overall statistics
    stats_match = re.search(
        r'Organism pages \(all\): (\d+) unique, (\d+) visitors, (\d+) pageviews\n'
        r'Organism pages \(with no assembly page visits\): (\d+) unique, (\d+) visitors, (\d+) pageviews\n'
        r'Priority pathogen pages: (\d+) unique, (\d+) visitors, (\d+) pageviews\n'
        r'Assembly pages \(all\): (\d+) unique, (\d+) visitors, (\d+) pageviews\n'
        r'Assembly pages \(with no workflow page visits\): (\d+) unique, (\d+) visitors, (\d+) pageviews',
        content
    )
    if stats_match:
        data['overall_stats'] = {
            'organism_all': {'unique': int(stats_match.group(1)), 'visitors': int(stats_match.group(2)), 'pageviews': int(stats_match.group(3))},
            'organism_no_assembly': {'unique': int(stats_match.group(4)), 'visitors': int(stats_match.group(5)), 'pageviews': int(stats_match.group(6))},
            'priority_pathogens': {'unique': int(stats_match.group(7)), 'visitors': int(stats_match.group(8)), 'pageviews': int(stats_match.group(9))},
            'assembly_all': {'unique': int(stats_match.group(10)), 'visitors': int(stats_match.group(11)), 'pageviews': int(stats_match.group(12))},
            'assembly_no_workflow': {'unique': int(stats_match.group(13)), 'visitors': int(stats_match.group(14)), 'pageviews': int(stats_match.group(15))},
        }
    
    # Parse high-level pages
    hl_section = re.search(r'HIGH-LEVEL NAVIGATION PAGES\n-+\n.*?\n-+\n(.*?)(?=\n\n|\Z)', content, re.DOTALL)
    if hl_section:
        for line in hl_section.group(1).strip().split('\n'):
            parts = line.split()
            if len(parts) >= 4 and parts[0].startswith('/'):
                data['high_level_pages'].append({
                    'url': parts[0],
                    'visitors': int(parts[1]),
                    'pageviews': int(parts[2]),
                    'bounce_rate': parts[3] if len(parts) > 3 else 'N/A',
                    'avg_time': ' '.join(parts[4:]) if len(parts) > 4 else 'N/A',
                })
    
    # Parse organism pages (all)
    org_section = re.search(r'ORGANISM PAGES \(All - Regardless of Assembly Status\)\n-+\n.*?\n-+\n(.*?)(?=\n\nORGANISM PAGES \(Where|\Z)', content, re.DOTALL)
    if org_section:
        for line in org_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format: Tax ID, Organism name, Visitors, Pageviews, Avg Time
            match = re.match(r'^(\d+)\s+(.+?)\s+(\d+)\s+(\d+)\s+(.*)$', line)
            if match:
                data['organism_pages_all'].append({
                    'tax_id': match.group(1),
                    'organism': match.group(2).strip(),
                    'visitors': int(match.group(3)),
                    'pageviews': int(match.group(4)),
                    'avg_time': match.group(5).strip() or 'N/A',
                })
    
    # Parse assembly pages (all)
    asm_section = re.search(r'ASSEMBLY PAGES \(All - Regardless of Workflow Status\)\n-+\n.*?\n-+\n(.*?)(?=\n\nASSEMBLY PAGES \(Where|\Z)', content, re.DOTALL)
    if asm_section:
        for line in asm_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format: Assembly ID, Organism name, Visitors, Pageviews, Avg Time, [*]
            match = re.match(r'^(\S+)\s+(.+?)\s+(\d+)\s+(\d+)\s+(.*)$', line)
            if match:
                data['assembly_pages_all'].append({
                    'assembly_id': match.group(1),
                    'organism': match.group(2).strip(),
                    'visitors': int(match.group(3)),
                    'pageviews': int(match.group(4)),
                    'avg_time': match.group(5).strip().rstrip(' *') or 'N/A',
                    'first_bias': '*' in match.group(5),
                })
    
    return data


def parse_workflow_analysis(filepath):
    """Parse a workflow analysis text file."""
    data = {
        'title': 'Workflow Configuration Page Analysis',
        'date_range': '',
        'overall_stats': {},
        'workflows': [],  # Per-workflow breakdown
        'workflow_organism': [],  # Workflow-organism intersections
        'assemblies': [],  # Per-assembly breakdown
    }
    
    match = re.search(r'(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})', filepath.name)
    if match:
        data['date_range'] = f"{match.group(1)} to {match.group(2)}"
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Parse overall statistics
    unique_match = re.search(r'Total unique assemblies with workflow visits: (\d+)', content)
    workflows_match = re.search(r'Total unique workflows: (\d+)', content)
    visitors_match = re.search(r'Total visitors to workflow pages: (\d+)', content)
    pageviews_match = re.search(r'Total pageviews: (\d+)', content)
    
    data['overall_stats'] = {
        'total': {
            'unique': int(unique_match.group(1)) if unique_match else 0,
            'workflows': int(workflows_match.group(1)) if workflows_match else 0,
            'visitors': int(visitors_match.group(1)) if visitors_match else 0,
            'pageviews': int(pageviews_match.group(1)) if pageviews_match else 0,
        }
    }
    
    # Parse per-workflow breakdown
    wf_section = re.search(r'PER-WORKFLOW BREAKDOWN\n-+\n.*?\n-+\n(.*?)(?=\n\n|\Z)', content, re.DOTALL)
    if wf_section:
        for line in wf_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format: Workflow (36 chars), Visitors, Pageviews, Assemblies, Avg Time, Median Time
            # Use regex to extract: workflow name, then 3 numbers (visitors, pageviews, assemblies)
            # Time values can be like "21s", "2m 53s", "N/A"
            match = re.match(r'^(\S+(?:\.\.\.)?)[\s]+(\d+)\s+(\d+)\s+(\d+)', line.strip())
            if match:
                workflow = match.group(1)
                visitors = int(match.group(2))
                pageviews = int(match.group(3))
                assemblies = int(match.group(4))
                
                data['workflows'].append({
                    'workflow': workflow,
                    'visitors': visitors,
                    'pageviews': pageviews,
                    'assemblies': assemblies,
                })
    
    # Parse workflow-organism intersections
    # The format uses fixed-width columns, so we need to parse by position
    wo_section = re.search(r'WORKFLOW-ORGANISM INTERSECTIONS.*?\n-+\n.*?\n-+\n(.*?)(?=\n\n|\Z)', content, re.DOTALL)
    if wo_section:
        for line in wo_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format is fixed-width: Workflow (30 chars), Organism (30 chars), Visitors, Pageviews
            # But we can parse by finding the last two numbers
            parts = line.split()
            if len(parts) >= 4:
                try:
                    pageviews = int(parts[-1])
                    visitors = int(parts[-2])
                    # The text file uses fixed columns - workflow is ~30 chars, organism is ~30 chars
                    # Find where the numbers start by looking at the line
                    # Everything before the last two numbers is workflow + organism
                    remaining_text = line.rsplit(None, 2)[0]  # Remove last 2 numbers
                    # Split at roughly the middle (30 char boundary)
                    if len(remaining_text) > 30:
                        workflow = remaining_text[:30].strip()
                        organism = remaining_text[30:].strip()
                    else:
                        workflow = remaining_text.strip()
                        organism = 'Unknown'
                    
                    data['workflow_organism'].append({
                        'workflow': workflow,
                        'organism': organism,
                        'visitors': visitors,
                        'pageviews': pageviews,
                    })
                except (ValueError, IndexError):
                    continue
    
    # Parse per-assembly breakdown
    asm_section = re.search(r'PER-ASSEMBLY BREAKDOWN\n-+\n.*?\n-+\n(.*?)(?=\n\n|\Z)', content, re.DOTALL)
    if asm_section:
        for line in asm_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format: Assembly ID, Organism, Visitors, Pageviews, Avg Time, Median Time, [*]
            parts = line.split()
            if len(parts) >= 4:
                try:
                    # Work backwards, skip N/A and *
                    idx = len(parts) - 1
                    while idx >= 0 and (parts[idx] == 'N/A' or parts[idx] == '*'):
                        idx -= 1
                    pageviews = int(parts[idx]) if idx >= 0 and parts[idx].isdigit() else 0
                    idx -= 1
                    visitors = int(parts[idx]) if idx >= 0 and parts[idx].isdigit() else 0
                    idx -= 1
                    assembly_id = parts[0]
                    organism = ' '.join(parts[1:idx+1])
                    
                    data['assemblies'].append({
                        'assembly_id': assembly_id,
                        'organism': organism,
                        'visitors': visitors,
                        'pageviews': pageviews,
                    })
                except (ValueError, IndexError):
                    continue
    
    return data


def generate_organism_html(data, output_path):
    """Generate HTML for organism analysis with community-grouped bar charts."""
    
    # Classify organisms by community
    organisms_by_community = {c: [] for c in COMMUNITIES_ORDER}
    for o in data['organism_pages_all']:
        tax_id = o['tax_id']
        name, lineage = _taxonomy_cache.get(tax_id, ('Unknown', 'Unknown'))
        community = classify_community(lineage)
        organisms_by_community[community].append({
            **o,
            'lineage': lineage,
            'community': community
        })
    
    # Classify assemblies by community
    assemblies_by_community = {c: [] for c in COMMUNITIES_ORDER}
    for a in data['assembly_pages_all']:
        assembly_id = a['assembly_id']
        _, name, lineage = _assembly_taxonomy_cache.get(assembly_id, (None, 'Unknown', 'Unknown'))
        community = classify_community(lineage)
        assemblies_by_community[community].append({
            **a,
            'lineage': lineage,
            'community': community
        })
    
    # Prepare high-level pages chart data
    hl_labels = json.dumps([p['url'] for p in data['high_level_pages']])
    hl_visitors = [p['visitors'] for p in data['high_level_pages']]
    hl_pageviews = [p['pageviews'] for p in data['high_level_pages']]
    
    # Prepare organism chart data - ALL organisms grouped by community
    org_chart_data = []
    for comm in COMMUNITIES_ORDER:
        for o in organisms_by_community[comm]:  # No limit - include all
            org_chart_data.append({
                'label': o['organism'][:25] + ('...' if len(o['organism']) > 25 else ''),
                'visitors': o['visitors'],
                'community': comm,
                'color': COMMUNITY_COLORS[comm]
            })
    
    org_labels = json.dumps([d['label'] for d in org_chart_data])
    org_visitors = [d['visitors'] for d in org_chart_data]
    org_colors = json.dumps([d['color'] for d in org_chart_data])
    
    # Prepare assembly chart data - ALL assemblies grouped by community
    asm_chart_data = []
    for comm in COMMUNITIES_ORDER:
        for a in assemblies_by_community[comm]:  # No limit - include all
            asm_chart_data.append({
                'label': a['organism'][:25] + ('...' if len(a['organism']) > 25 else ''),
                'visitors': a['visitors'],
                'community': comm,
                'color': COMMUNITY_COLORS[comm]
            })
    
    asm_labels = json.dumps([d['label'] for d in asm_chart_data])
    asm_visitors = [d['visitors'] for d in asm_chart_data]
    asm_colors = json.dumps([d['color'] for d in asm_chart_data])
    
    # Generate legend items for communities
    legend_items = ' '.join([
        f'<span style="display:inline-flex;align-items:center;margin-right:16px;">'
        f'<span style="width:12px;height:12px;background:{COMMUNITY_COLORS[c]};border-radius:2px;margin-right:4px;"></span>'
        f'{c}</span>'
        for c in COMMUNITIES_ORDER if organisms_by_community[c] or assemblies_by_community[c]
    ])
    
    # Top organisms table (all, sorted by visitors)
    top_organisms = sorted(data['organism_pages_all'], key=lambda x: x['visitors'], reverse=True)[:20]
    organism_rows = '\n'.join([
        f'''<tr>
            <td><a href="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={o['tax_id']}" target="_blank">{o['tax_id']}</a></td>
            <td>{o['organism']}</td>
            <td><span style="color:{COMMUNITY_COLORS.get(organisms_by_community_lookup.get(o['tax_id'], 'Other'), '#6b7280')}">{organisms_by_community_lookup.get(o['tax_id'], 'Other')}</span></td>
            <td class="num">{o['visitors']}</td>
            <td class="num">{o['pageviews']}</td>
        </tr>'''
        for o in top_organisms
        for organisms_by_community_lookup in [{org['tax_id']: org['community'] for comm in COMMUNITIES_ORDER for org in organisms_by_community[comm]}]
    ])
    
    # Top assemblies table
    top_assemblies = sorted(data['assembly_pages_all'], key=lambda x: x['visitors'], reverse=True)[:20]
    assemblies_lookup = {asm['assembly_id']: asm['community'] for comm in COMMUNITIES_ORDER for asm in assemblies_by_community[comm]}
    assembly_rows = '\n'.join([
        f'''<tr>
            <td><a href="https://www.ncbi.nlm.nih.gov/datasets/genome/{a['assembly_id'].replace('_', '.')}" target="_blank">{a['assembly_id']}</a></td>
            <td>{a['organism']}</td>
            <td><span style="color:{COMMUNITY_COLORS.get(assemblies_lookup.get(a['assembly_id'], 'Other'), '#6b7280')}">{assemblies_lookup.get(a['assembly_id'], 'Other')}</span></td>
            <td class="num">{a['visitors']}</td>
            <td class="num">{a['pageviews']}</td>
        </tr>'''
        for a in top_assemblies
    ])
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Organism Analysis - {data['date_range']}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f8fafc;
            color: #1e293b;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0 0 10px 0; color: #0f172a; }}
        .subtitle {{ color: #64748b; font-size: 14px; }}
        .legend {{ margin-top: 15px; font-size: 13px; color: #475569; }}
        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
        }}
        .chart-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .chart-container.small {{ height: 300px; }}
        .chart-container.medium {{ height: 400px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid #e2e8f0;
        }}
        th {{ background: #f8fafc; font-weight: 600; color: #475569; }}
        tr:hover {{ background: #f8fafc; }}
        .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Organism & Pathogen Page Analysis</h1>
        <div class="subtitle">{data['date_range']}</div>
        <div class="legend">{legend_items}</div>
    </div>
    
    <div class="section">
        <h2 class="section-title">High-Level Navigation Pages</h2>
        <div class="chart-container small">
            <canvas id="hlChart"></canvas>
        </div>
    </div>
    
    <div class="section">
        <h2 class="section-title">Top Organism Pages by Community</h2>
        <div class="chart-container medium">
            <canvas id="orgChart"></canvas>
        </div>
        <table>
            <thead>
                <tr><th>Tax ID</th><th>Organism</th><th>Community</th><th class="num">Visitors</th><th class="num">Pageviews</th></tr>
            </thead>
            <tbody>{organism_rows}</tbody>
        </table>
    </div>
    
    <div class="section">
        <h2 class="section-title">Top Assembly Pages by Community</h2>
        <div class="chart-container medium">
            <canvas id="asmChart"></canvas>
        </div>
        <table>
            <thead>
                <tr><th>Assembly ID</th><th>Organism</th><th>Community</th><th class="num">Visitors</th><th class="num">Pageviews</th></tr>
            </thead>
            <tbody>{assembly_rows}</tbody>
        </table>
    </div>
    
    <script>
        // High-level pages chart
        new Chart(document.getElementById('hlChart'), {{
            type: 'bar',
            data: {{
                labels: {hl_labels},
                datasets: [
                    {{
                        label: 'Visitors',
                        data: {hl_visitors},
                        backgroundColor: '#2563eb'
                    }},
                    {{
                        label: 'Pageviews',
                        data: {hl_pageviews},
                        backgroundColor: '#7c3aed'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{ display: true, text: 'High-Level Navigation Pages' }},
                    legend: {{ position: 'bottom' }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});
        
        // Organism pages chart
        new Chart(document.getElementById('orgChart'), {{
            type: 'bar',
            data: {{
                labels: {org_labels},
                datasets: [{{
                    label: 'Visitors',
                    data: {org_visitors},
                    backgroundColor: {org_colors}
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{ display: true, text: 'Top Organisms by Visitors (colored by community)' }},
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }},
                    x: {{ ticks: {{ maxRotation: 45, minRotation: 45 }} }}
                }}
            }}
        }});
        
        // Assembly pages chart
        new Chart(document.getElementById('asmChart'), {{
            type: 'bar',
            data: {{
                labels: {asm_labels},
                datasets: [{{
                    label: 'Visitors',
                    data: {asm_visitors},
                    backgroundColor: {asm_colors}
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{ display: true, text: 'Top Assemblies by Visitors (colored by community)' }},
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }},
                    x: {{ ticks: {{ maxRotation: 45, minRotation: 45 }} }}
                }}
            }}
        }});
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w') as f:
        f.write(html)


def generate_workflow_html(data, output_path):
    """Generate HTML for workflow analysis with network diagram and community-grouped charts."""
    stats = data['overall_stats']
    
    # Use per-workflow breakdown for chart - show ALL workflows
    sorted_workflows = sorted(data['workflows'], key=lambda x: x['visitors'], reverse=True)
    
    chart_labels = json.dumps([wf['workflow'][:30] for wf in sorted_workflows])
    chart_visitors = [wf['visitors'] for wf in sorted_workflows]
    chart_pageviews = [wf['pageviews'] for wf in sorted_workflows]
    
    # Build network data for workflow-organism bipartite graph
    # Nodes: workflows (type: 'workflow') and organisms (type: 'organism')
    # Edges: connections with visitor counts
    workflow_nodes = {}
    organism_nodes = {}
    edges = []
    
    for wo in data['workflow_organism']:
        wf_name = wo['workflow'][:25]
        org_name = wo['organism'][:25]
        visitors = wo['visitors']
        
        if wf_name not in workflow_nodes:
            workflow_nodes[wf_name] = {'visitors': 0}
        workflow_nodes[wf_name]['visitors'] += visitors
        
        if org_name not in organism_nodes:
            organism_nodes[org_name] = {'visitors': 0}
        organism_nodes[org_name]['visitors'] += visitors
        
        edges.append({
            'source': wf_name,
            'target': org_name,
            'visitors': visitors
        })
    
    network_data = {
        'workflows': [{'id': k, 'visitors': v['visitors']} for k, v in workflow_nodes.items()],
        'organisms': [{'id': k, 'visitors': v['visitors']} for k, v in organism_nodes.items()],
        'edges': edges
    }
    network_json = json.dumps(network_data)
    
    # Classify assemblies by community for bar chart - include ALL assemblies
    assemblies_by_community = {c: [] for c in COMMUNITIES_ORDER}
    for a in data['assemblies']:
        assembly_id = a['assembly_id']
        _, name, lineage = _assembly_taxonomy_cache.get(assembly_id, (None, 'Unknown', 'Unknown'))
        community = classify_community(lineage)
        assemblies_by_community[community].append({
            **a,
            'community': community
        })
    
    # Prepare assembly chart data - ALL assemblies grouped by community
    asm_chart_data = []
    for comm in COMMUNITIES_ORDER:
        for a in assemblies_by_community[comm]:  # No limit - include all
            asm_chart_data.append({
                'label': a['organism'][:20] + ('...' if len(a['organism']) > 20 else ''),
                'visitors': a['visitors'],
                'community': comm,
                'color': COMMUNITY_COLORS[comm]
            })
    
    asm_labels = json.dumps([d['label'] for d in asm_chart_data])
    asm_visitors = [d['visitors'] for d in asm_chart_data]
    asm_colors = json.dumps([d['color'] for d in asm_chart_data])
    
    # Generate legend items for communities
    legend_items = ' '.join([
        f'<span style="display:inline-flex;align-items:center;margin-right:16px;">'
        f'<span style="width:12px;height:12px;background:{COMMUNITY_COLORS[c]};border-radius:2px;margin-right:4px;"></span>'
        f'{c}</span>'
        for c in COMMUNITIES_ORDER if assemblies_by_community[c]
    ])
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Workflow Analysis - {data['date_range']}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f8fafc;
            color: #1e293b;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0 0 10px 0; color: #0f172a; }}
        .subtitle {{ color: #64748b; font-size: 14px; }}
        .legend {{ margin-top: 15px; font-size: 13px; color: #475569; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{ font-size: 32px; font-weight: bold; color: #db2777; }}
        .stat-label {{ color: #64748b; font-size: 14px; margin-top: 5px; }}
        .chart-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .chart-container.bar {{ height: 350px; }}
        .chart-container.network {{ height: 600px; position: relative; }}
        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
        }}
        #networkSvg {{ width: 100%; height: 100%; }}
        .node-workflow {{ fill: #db2777; }}
        .node-organism {{ fill: #2563eb; }}
        .node-label {{ font-size: 10px; fill: #1e293b; pointer-events: none; }}
        .link {{ stroke: #94a3b8; stroke-opacity: 0.6; }}
        .network-legend {{ position: absolute; top: 10px; right: 10px; font-size: 12px; }}
        .network-legend span {{ display: inline-flex; align-items: center; margin-left: 12px; }}
        .network-legend .dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Workflow Configuration Page Analysis</h1>
        <div class="subtitle">{data['date_range']}</div>
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('unique', 0)}</div>
            <div class="stat-label">Assemblies with Workflow Visits</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('workflows', 0)}</div>
            <div class="stat-label">Unique Workflows</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('visitors', 0)}</div>
            <div class="stat-label">Total Visitors</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('pageviews', 0)}</div>
            <div class="stat-label">Total Pageviews</div>
        </div>
    </div>
    
    <div class="section">
        <h2 class="section-title">Visitors & Pageviews by Workflow Type</h2>
        <div class="chart-container bar">
            <canvas id="workflowChart"></canvas>
        </div>
    </div>
    
    <div class="section">
        <h2 class="section-title">Workflow-Organism Network</h2>
        <div class="chart-container network">
            <div class="network-legend">
                <span><span class="dot" style="background:#db2777;"></span>Workflow</span>
                <span><span class="dot" style="background:#2563eb;"></span>Organism</span>
                <span style="margin-left:20px;color:#64748b;font-style:italic;">Node size = total visitors | Edge width = visitors for connection</span>
            </div>
            <svg id="networkSvg"></svg>
        </div>
    </div>
    
    <div class="section">
        <h2 class="section-title">Workflow Page Visitors by Assembly (grouped by community)</h2>
        <div class="legend" style="margin-bottom:15px;">{legend_items}</div>
        <div class="chart-container bar">
            <canvas id="assemblyChart"></canvas>
        </div>
    </div>
    
    <script>
        // Workflow bar chart
        new Chart(document.getElementById('workflowChart'), {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [
                    {{
                        label: 'Visitors',
                        data: {chart_visitors},
                        backgroundColor: '#db2777'
                    }},
                    {{
                        label: 'Pageviews',
                        data: {chart_pageviews},
                        backgroundColor: '#7c3aed'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{
                    legend: {{ position: 'bottom' }}
                }}
            }}
        }});
        
        // Assembly bar chart by community
        new Chart(document.getElementById('assemblyChart'), {{
            type: 'bar',
            data: {{
                labels: {asm_labels},
                datasets: [{{
                    label: 'Visitors',
                    data: {asm_visitors},
                    backgroundColor: {asm_colors}
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }},
                    x: {{ ticks: {{ maxRotation: 45, minRotation: 45 }} }}
                }}
            }}
        }});
        
        // Network diagram using D3.js
        const networkData = {network_json};
        
        if (networkData.workflows.length > 0 && networkData.organisms.length > 0) {{
            const svg = d3.select('#networkSvg');
            const container = document.querySelector('.chart-container.network');
            const width = container.clientWidth - 40;
            const height = container.clientHeight - 60;
            
            svg.attr('width', width).attr('height', height);
            
            // Create a group for zoom/pan
            const g = svg.append('g');
            
            // Add zoom behavior
            const zoom = d3.zoom()
                .scaleExtent([0.2, 3])
                .on('zoom', (event) => g.attr('transform', event.transform));
            svg.call(zoom);
            
            // Create nodes array
            const nodes = [
                ...networkData.workflows.map(w => ({{ id: w.id, type: 'workflow', visitors: w.visitors }})),
                ...networkData.organisms.map(o => ({{ id: o.id, type: 'organism', visitors: o.visitors }}))
            ];
            
            // Create links array
            const links = networkData.edges.map(e => ({{
                source: e.source,
                target: e.target,
                visitors: e.visitors
            }}));
            
            // Scale for node sizes - smaller nodes
            const maxVisitors = Math.max(...nodes.map(n => n.visitors));
            const nodeScale = d3.scaleSqrt().domain([1, maxVisitors]).range([5, 15]);
            
            // Scale for edge widths
            const maxEdgeVisitors = Math.max(...links.map(l => l.visitors));
            const edgeScale = d3.scaleLinear().domain([1, maxEdgeVisitors]).range([1, 5]);
            
            // Create simulation with tighter forces to keep graph compact
            const simulation = d3.forceSimulation(nodes)
                .force('link', d3.forceLink(links).id(d => d.id).distance(60))
                .force('charge', d3.forceManyBody().strength(-80))
                .force('center', d3.forceCenter(width / 2, height / 2))
                .force('collision', d3.forceCollide().radius(d => nodeScale(d.visitors) + 3))
                .force('x', d3.forceX(width / 2).strength(0.05))
                .force('y', d3.forceY(height / 2).strength(0.05));
            
            // Draw links
            const link = g.append('g')
                .selectAll('line')
                .data(links)
                .join('line')
                .attr('class', 'link')
                .attr('stroke-width', d => edgeScale(d.visitors));
            
            // Draw nodes
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
            
            // Add labels - smaller font
            const label = g.append('g')
                .selectAll('text')
                .data(nodes)
                .join('text')
                .attr('class', 'node-label')
                .attr('dy', d => nodeScale(d.visitors) + 10)
                .attr('text-anchor', 'middle')
                .text(d => d.id.length > 15 ? d.id.slice(0, 15) + '...' : d.id);
            
            // Add tooltips
            node.append('title')
                .text(d => `${{d.id}}\\n${{d.visitors}} visitors`);
            
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
            
            // After simulation settles, fit the graph to view
            simulation.on('end', () => {{
                const bounds = g.node().getBBox();
                const fullWidth = width;
                const fullHeight = height;
                const bWidth = bounds.width;
                const bHeight = bounds.height;
                const scale = 0.85 / Math.max(bWidth / fullWidth, bHeight / fullHeight);
                const tx = (fullWidth - scale * (bounds.x * 2 + bWidth)) / 2;
                const ty = (fullHeight - scale * (bounds.y * 2 + bHeight)) / 2;
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
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w') as f:
        f.write(html)


def process_file(filepath):
    """Process a single analysis file."""
    filepath = Path(filepath)
    
    if not filepath.exists():
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        return False
    
    output_path = filepath.with_suffix('.html')
    
    if 'organism-analysis' in filepath.name:
        print(f"Processing organism analysis: {filepath.name}")
        data = parse_organism_analysis(filepath)
        generate_organism_html(data, output_path)
        print(f"  -> {output_path.name}")
        return True
    elif 'workflow-analysis' in filepath.name:
        print(f"Processing workflow analysis: {filepath.name}")
        data = parse_workflow_analysis(filepath)
        generate_workflow_html(data, output_path)
        print(f"  -> {output_path.name}")
        return True
    else:
        print(f"Skipping unknown file type: {filepath.name}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Generate HTML reports from analysis text files")
    parser.add_argument('path', help="Path to analysis file or directory containing analysis files")
    args = parser.parse_args()
    
    path = Path(args.path)
    
    # Load taxonomy cache from project root
    script_dir = Path(__file__).parent
    cache_file = script_dir.parent / '.taxonomy_cache.json'
    if cache_file.exists():
        load_taxonomy_cache(cache_file)
        print(f"Loaded taxonomy cache ({len(_taxonomy_cache)} taxa, {len(_assembly_taxonomy_cache)} assemblies)", file=sys.stderr)
    else:
        print("Warning: No taxonomy cache found. Run generate_monthly_summary_html.py first to build cache.", file=sys.stderr)
    
    if path.is_file():
        process_file(path)
    elif path.is_dir():
        # Process all analysis files in directory
        files = list(path.glob('*-analysis.txt'))
        if not files:
            print(f"No analysis files found in {path}", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(files)} analysis files")
        for f in sorted(files):
            process_file(f)
        print(f"\nGenerated {len(files)} HTML reports")
    else:
        print(f"Error: Path not found: {path}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
