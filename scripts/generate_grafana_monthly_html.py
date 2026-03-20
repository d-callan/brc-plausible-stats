#!/usr/bin/env python3
"""
Generate per-month HTML reports for Grafana workflow landing data.

This script creates detailed HTML reports for each month of Grafana data,
showing workflow landings by community, category, and individual workflows.

Usage:
    python3 generate_grafana_monthly_html.py
    python3 generate_grafana_monthly_html.py data/fetched/
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

# Workflow category order (same as in generate_monthly_summary_html.py)
WORKFLOW_CATEGORIES_ORDER = ['Variant Calling', 'Transcription', 'Single Cell', 'Epigenomics', 'AMR', 'Viral', 'Other']

# Community order
COMMUNITIES_ORDER = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths', 'Other']

# Colors for charts
COLORS = {
    'Viruses': '#dc2626',
    'Bacteria': '#2563eb',
    'Fungi': '#65a30d',
    'Protists': '#7c3aed',
    'Vectors': '#ea580c',
    'Hosts': '#0891b2',
    'Helminths': '#db2777',
    'Other': '#6b7280',
    'Variant Calling': '#dc2626',
    'Transcription': '#2563eb',
    'Single Cell': '#7c3aed',
    'Epigenomics': '#65a30d',
    'AMR': '#ea580c',
    'Viral': '#0891b2',
}


def get_grafana_files(data_dir):
    """Get all Grafana landing data files sorted by date."""
    files = []
    pattern = re.compile(r'grafana-landings-(\d{4})-(\d{2})-\d{2}-to-(\d{4})-(\d{2})-\d{2}\.json')
    
    for f in data_dir.glob('grafana-landings-*.json'):
        match = pattern.match(f.name)
        if match:
            year, month = int(match.group(1)), int(match.group(2))
            files.append((year, month, f))
    
    files.sort(key=lambda x: (x[0], x[1]))
    return files


def generate_html_report(data, output_path, month_label, date_range):
    """Generate an HTML report for a single month of Grafana data."""
    
    total_landings = data.get('total_landings', 0)
    by_community = data.get('by_community', {})
    by_category = data.get('by_category', {})
    by_workflow = data.get('by_workflow', {})
    by_dbkey = data.get('by_dbkey', {})
    
    # Sort by count descending
    sorted_workflows = sorted(by_workflow.items(), key=lambda x: x[1], reverse=True)
    sorted_dbkeys = sorted(by_dbkey.items(), key=lambda x: x[1], reverse=True)
    sorted_communities = [(c, by_community.get(c, 0)) for c in COMMUNITIES_ORDER if by_community.get(c, 0) > 0]
    sorted_categories = [(c, by_category.get(c, 0)) for c in WORKFLOW_CATEGORIES_ORDER if by_category.get(c, 0) > 0]
    
    # Generate workflow table rows
    workflow_rows = []
    for i, (name, count) in enumerate(sorted_workflows[:50], 1):
        workflow_rows.append(f'''
            <tr>
                <td>{i}</td>
                <td>{name}</td>
                <td>{count:,}</td>
            </tr>
        ''')
    
    # Generate dbkey table rows
    dbkey_rows = []
    for i, (dbkey, count) in enumerate(sorted_dbkeys[:50], 1):
        dbkey_rows.append(f'''
            <tr>
                <td>{i}</td>
                <td><code>{dbkey}</code></td>
                <td>{count:,}</td>
            </tr>
        ''')
    
    # Generate chart data
    community_labels = json.dumps([c for c, _ in sorted_communities])
    community_data = json.dumps([count for _, count in sorted_communities])
    community_colors = json.dumps([COLORS.get(c, '#6b7280') for c, _ in sorted_communities])
    
    category_labels = json.dumps([c for c, _ in sorted_categories])
    category_data = json.dumps([count for _, count in sorted_categories])
    category_colors = json.dumps([COLORS.get(c, '#6b7280') for c, _ in sorted_categories])
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Galaxy Workflow Landings - {month_label}</title>
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
            margin-bottom: 30px;
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
        .back-link {{
            display: inline-block;
            margin-bottom: 20px;
            color: #2563eb;
            text-decoration: none;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{
            font-size: 32px;
            font-weight: 700;
            color: #10b981;
        }}
        .stat-label {{
            color: #64748b;
            font-size: 14px;
            margin-top: 4px;
        }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 24px;
            margin-bottom: 30px;
        }}
        .chart-container {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            height: 350px;
        }}
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            margin: 30px 0 15px 0;
            padding-bottom: 8px;
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
        th {{
            background: #f8fafc;
            font-weight: 600;
            color: #475569;
        }}
        tr:hover {{
            background: #f8fafc;
        }}
        code {{
            background: #f1f5f9;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 13px;
        }}
        .tables-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
        }}
        .data-source-note {{
            background: #ecfdf5;
            border: 1px solid #a7f3d0;
            border-radius: 8px;
            padding: 12px 16px;
            margin-bottom: 20px;
            font-size: 14px;
            color: #065f46;
        }}
    </style>
</head>
<body>
    <a href="../index.html" class="back-link">← Back to Summary</a>
    
    <div class="header">
        <h1>Galaxy Workflow Landings - {month_label}</h1>
        <div class="subtitle">Data from Grafana/InfluxDB | Date range: {date_range[0]} to {date_range[1]}</div>
    </div>
    
    <div class="data-source-note">
        <strong>Data Source:</strong> Galaxy workflow landing requests originating from BRC Analytics (brc-analytics.org).
        These are actual workflow launches in Galaxy, not just page views.
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{total_landings:,}</div>
            <div class="stat-label">Total Workflow Landings</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{len(by_workflow)}</div>
            <div class="stat-label">Unique Workflows</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{len(by_dbkey)}</div>
            <div class="stat-label">Unique Assemblies (dbkeys)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{len([c for c in by_community.values() if c > 0])}</div>
            <div class="stat-label">Communities with Landings</div>
        </div>
    </div>
    
    <div class="charts-grid">
        <div class="chart-container">
            <canvas id="communityChart"></canvas>
        </div>
        <div class="chart-container">
            <canvas id="categoryChart"></canvas>
        </div>
    </div>
    
    <h2 class="section-title">Top Workflows</h2>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Workflow Name</th>
                <th>Landings</th>
            </tr>
        </thead>
        <tbody>
            {''.join(workflow_rows) if workflow_rows else '<tr><td colspan="3">No workflow data available</td></tr>'}
        </tbody>
    </table>
    
    <h2 class="section-title">Top Assemblies (dbkeys)</h2>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Assembly ID (dbkey)</th>
                <th>Landings</th>
            </tr>
        </thead>
        <tbody>
            {''.join(dbkey_rows) if dbkey_rows else '<tr><td colspan="3">No assembly data available</td></tr>'}
        </tbody>
    </table>
    
    <script>
        // Community chart
        new Chart(document.getElementById('communityChart'), {{
            type: 'bar',
            data: {{
                labels: {community_labels},
                datasets: [{{
                    label: 'Landings',
                    data: {community_data},
                    backgroundColor: {community_colors}
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Landings by Community',
                        font: {{ size: 16, weight: 'bold' }}
                    }},
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});
        
        // Category chart
        new Chart(document.getElementById('categoryChart'), {{
            type: 'bar',
            data: {{
                labels: {category_labels},
                datasets: [{{
                    label: 'Landings',
                    data: {category_data},
                    backgroundColor: {category_colors}
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Landings by Workflow Category',
                        font: {{ size: 16, weight: 'bold' }}
                    }},
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w') as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser(description="Generate per-month Grafana landing HTML reports")
    parser.add_argument('data_dir', nargs='?', default='data/fetched',
                        help="Directory containing Grafana JSON files (default: data/fetched)")
    parser.add_argument('--output-dir', '-o', default='output/fetched',
                        help="Output directory for HTML files (default: output/fetched)")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    
    # Resolve paths
    data_dir = Path(args.data_dir)
    if not data_dir.is_absolute():
        data_dir = script_dir.parent / data_dir
    
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = script_dir.parent / output_dir
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all Grafana files
    grafana_files = get_grafana_files(data_dir)
    
    if not grafana_files:
        print("No Grafana landing data files found", file=sys.stderr)
        print(f"  Looked in: {data_dir}", file=sys.stderr)
        print("  Run: python3 scripts/fetch_grafana_landings.py to fetch data", file=sys.stderr)
        return
    
    print(f"Found {len(grafana_files)} Grafana data files", file=sys.stderr)
    
    generated_count = 0
    
    for year, month, filepath in grafana_files:
        month_label = datetime(year, month, 1).strftime('%B %Y')
        
        try:
            with open(filepath, 'r') as f:
                raw_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Warning: Could not load {filepath.name}: {e}", file=sys.stderr)
            continue
        
        # Skip months with no data
        if raw_data.get('summary', {}).get('total_landings', 0) == 0:
            print(f"  Skipping {month_label} (no landings)", file=sys.stderr)
            continue
        
        # Extract data
        metadata = raw_data.get('metadata', {})
        date_range = (metadata.get('start_date', 'unknown'), metadata.get('end_date', 'unknown'))
        
        data = {
            'total_landings': raw_data.get('summary', {}).get('total_landings', 0),
            'by_community': raw_data.get('by_community', {}),
            'by_category': raw_data.get('by_category', {}),
            'by_workflow': raw_data.get('by_workflow', {}),
            'by_dbkey': raw_data.get('by_dbkey', {}),
        }
        
        # Generate output filename
        output_filename = f"grafana-landings-{year}-{month:02d}.html"
        output_path = output_dir / output_filename
        
        print(f"  Generating {month_label}...", file=sys.stderr)
        generate_html_report(data, output_path, month_label, date_range)
        generated_count += 1
    
    print(f"\nGenerated {generated_count} HTML reports in {output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
