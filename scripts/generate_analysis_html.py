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
import re
import sys
from pathlib import Path


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
        'workflows': [],
    }
    
    match = re.search(r'(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})', filepath.name)
    if match:
        data['date_range'] = f"{match.group(1)} to {match.group(2)}"
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Parse overall statistics
    stats_match = re.search(r'Total workflow pages: (\d+) unique, (\d+) visitors, (\d+) pageviews', content)
    if stats_match:
        data['overall_stats'] = {
            'total': {'unique': int(stats_match.group(1)), 'visitors': int(stats_match.group(2)), 'pageviews': int(stats_match.group(3))},
        }
    
    # Parse workflow entries
    wf_section = re.search(r'WORKFLOW CONFIGURATION PAGES\n-+\n.*?\n-+\n(.*?)(?=\n\n|\Z)', content, re.DOTALL)
    if wf_section:
        for line in wf_section.group(1).strip().split('\n'):
            if not line.strip():
                continue
            # Format: Assembly ID, Workflow, Organism, Visitors, Pageviews, Avg Time
            parts = line.split()
            if len(parts) >= 5:
                # Find the numeric columns from the end
                try:
                    avg_time_parts = []
                    idx = len(parts) - 1
                    # Work backwards to find avg time
                    while idx >= 0 and not parts[idx].isdigit():
                        avg_time_parts.insert(0, parts[idx])
                        idx -= 1
                    pageviews = int(parts[idx]) if idx >= 0 else 0
                    idx -= 1
                    visitors = int(parts[idx]) if idx >= 0 else 0
                    idx -= 1
                    # Everything before is assembly_id, workflow, organism
                    remaining = parts[:idx+1]
                    if len(remaining) >= 2:
                        assembly_id = remaining[0]
                        workflow = remaining[1]
                        organism = ' '.join(remaining[2:]) if len(remaining) > 2 else 'Unknown'
                        data['workflows'].append({
                            'assembly_id': assembly_id,
                            'workflow': workflow,
                            'organism': organism,
                            'visitors': visitors,
                            'pageviews': pageviews,
                            'avg_time': ' '.join(avg_time_parts) or 'N/A',
                        })
                except (ValueError, IndexError):
                    continue
    
    return data


def generate_organism_html(data, output_path):
    """Generate HTML for organism analysis."""
    stats = data['overall_stats']
    
    # Prepare chart data
    chart_labels = ['Organism Pages', 'Assembly Pages']
    chart_visitors = [
        stats.get('organism_all', {}).get('visitors', 0),
        stats.get('assembly_all', {}).get('visitors', 0),
    ]
    chart_pageviews = [
        stats.get('organism_all', {}).get('pageviews', 0),
        stats.get('assembly_all', {}).get('pageviews', 0),
    ]
    
    # Top organisms table
    top_organisms = data['organism_pages_all'][:20]
    organism_rows = '\n'.join([
        f'''<tr>
            <td><a href="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={o['tax_id']}" target="_blank">{o['tax_id']}</a></td>
            <td>{o['organism']}</td>
            <td class="num">{o['visitors']}</td>
            <td class="num">{o['pageviews']}</td>
            <td>{o['avg_time']}</td>
        </tr>'''
        for o in top_organisms
    ])
    
    # Top assemblies table
    top_assemblies = data['assembly_pages_all'][:20]
    assembly_rows = '\n'.join([
        f'''<tr>
            <td><a href="https://www.ncbi.nlm.nih.gov/datasets/genome/{a['assembly_id'].replace('_', '.')}" target="_blank">{a['assembly_id']}</a></td>
            <td>{a['organism']}</td>
            <td class="num">{a['visitors']}</td>
            <td class="num">{a['pageviews']}</td>
            <td>{a['avg_time']}</td>
        </tr>'''
        for a in top_assemblies
    ])
    
    # High-level pages table
    hl_rows = '\n'.join([
        f'''<tr>
            <td>{p['url']}</td>
            <td class="num">{p['visitors']}</td>
            <td class="num">{p['pageviews']}</td>
            <td>{p['bounce_rate']}</td>
            <td>{p['avg_time']}</td>
        </tr>'''
        for p in data['high_level_pages']
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
        .stat-value {{ font-size: 32px; font-weight: bold; color: #2563eb; }}
        .stat-label {{ color: #64748b; font-size: 14px; margin-top: 5px; }}
        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
        }}
        .charts-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .chart-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            height: 300px;
        }}
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
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{stats.get('organism_all', {}).get('unique', 0)}</div>
            <div class="stat-label">Organism Pages</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('organism_all', {}).get('visitors', 0)}</div>
            <div class="stat-label">Organism Visitors</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('assembly_all', {}).get('unique', 0)}</div>
            <div class="stat-label">Assembly Pages</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('assembly_all', {}).get('visitors', 0)}</div>
            <div class="stat-label">Assembly Visitors</div>
        </div>
    </div>
    
    <div class="charts-row">
        <div class="chart-container">
            <canvas id="visitorsChart"></canvas>
        </div>
        <div class="chart-container">
            <canvas id="pageviewsChart"></canvas>
        </div>
    </div>
    
    <div class="section">
        <h2 class="section-title">High-Level Navigation Pages</h2>
        <table>
            <thead>
                <tr><th>Page</th><th class="num">Visitors</th><th class="num">Pageviews</th><th>Bounce Rate</th><th>Avg Time</th></tr>
            </thead>
            <tbody>{hl_rows}</tbody>
        </table>
    </div>
    
    <div class="section">
        <h2 class="section-title">Top Organism Pages</h2>
        <table>
            <thead>
                <tr><th>Tax ID</th><th>Organism</th><th class="num">Visitors</th><th class="num">Pageviews</th><th>Avg Time</th></tr>
            </thead>
            <tbody>{organism_rows}</tbody>
        </table>
    </div>
    
    <div class="section">
        <h2 class="section-title">Top Assembly Pages</h2>
        <table>
            <thead>
                <tr><th>Assembly ID</th><th>Organism</th><th class="num">Visitors</th><th class="num">Pageviews</th><th>Avg Time</th></tr>
            </thead>
            <tbody>{assembly_rows}</tbody>
        </table>
    </div>
    
    <script>
        new Chart(document.getElementById('visitorsChart'), {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Visitors',
                    data: {chart_visitors},
                    backgroundColor: ['#2563eb', '#7c3aed']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: 'Visitors by Page Type' }} }}
            }}
        }});
        
        new Chart(document.getElementById('pageviewsChart'), {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Pageviews',
                    data: {chart_pageviews},
                    backgroundColor: ['#2563eb', '#7c3aed']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: 'Pageviews by Page Type' }} }}
            }}
        }});
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w') as f:
        f.write(html)


def generate_workflow_html(data, output_path):
    """Generate HTML for workflow analysis."""
    stats = data['overall_stats']
    
    # Group by workflow type
    workflow_counts = {}
    for wf in data['workflows']:
        wf_type = wf['workflow']
        if wf_type not in workflow_counts:
            workflow_counts[wf_type] = {'count': 0, 'visitors': 0, 'pageviews': 0}
        workflow_counts[wf_type]['count'] += 1
        workflow_counts[wf_type]['visitors'] += wf['visitors']
        workflow_counts[wf_type]['pageviews'] += wf['pageviews']
    
    # Sort by visitors
    sorted_workflows = sorted(workflow_counts.items(), key=lambda x: x[1]['visitors'], reverse=True)
    
    chart_labels = [wf[0] for wf in sorted_workflows[:10]]
    chart_visitors = [wf[1]['visitors'] for wf in sorted_workflows[:10]]
    
    # Workflow table
    wf_rows = '\n'.join([
        f'''<tr>
            <td>{wf['assembly_id']}</td>
            <td>{wf['workflow']}</td>
            <td>{wf['organism']}</td>
            <td class="num">{wf['visitors']}</td>
            <td class="num">{wf['pageviews']}</td>
        </tr>'''
        for wf in data['workflows'][:30]
    ])
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Workflow Analysis - {data['date_range']}</title>
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
            height: 350px;
            margin-bottom: 30px;
        }}
        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
        }}
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
            <div class="stat-label">Unique Workflow Pages</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('visitors', 0)}</div>
            <div class="stat-label">Total Visitors</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{stats.get('total', {}).get('pageviews', 0)}</div>
            <div class="stat-label">Total Pageviews</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{len(workflow_counts)}</div>
            <div class="stat-label">Workflow Types</div>
        </div>
    </div>
    
    <div class="chart-container">
        <canvas id="workflowChart"></canvas>
    </div>
    
    <div class="section">
        <h2 class="section-title">Workflow Pages</h2>
        <table>
            <thead>
                <tr><th>Assembly ID</th><th>Workflow</th><th>Organism</th><th class="num">Visitors</th><th class="num">Pageviews</th></tr>
            </thead>
            <tbody>{wf_rows}</tbody>
        </table>
    </div>
    
    <script>
        new Chart(document.getElementById('workflowChart'), {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Visitors',
                    data: {chart_visitors},
                    backgroundColor: '#db2777'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{ title: {{ display: true, text: 'Visitors by Workflow Type' }} }}
            }}
        }});
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
