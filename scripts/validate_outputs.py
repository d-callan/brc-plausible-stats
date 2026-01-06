#!/usr/bin/env python3
"""
Validate BRC analytics output files for structure and data integrity.

This script checks that generated HTML reports have the expected structure,
contain valid data, and haven't regressed from baseline expectations.

Usage:
    python validate_outputs.py                    # Validate current outputs
    python validate_outputs.py --create-baseline  # Create baseline snapshots
    python validate_outputs.py --verbose          # Show detailed output
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


def validate_html_structure(html_path, expected_sections):
    """Validate that HTML file has expected sections and structure."""
    if not html_path.exists():
        raise ValidationError(f"HTML file not found: {html_path}")
    
    with open(html_path, 'r') as f:
        content = f.read()
    
    errors = []
    
    # Check for expected sections
    for section in expected_sections:
        if section not in content:
            errors.append(f"Missing section: {section}")
    
    # Check for basic HTML structure
    if '<html' not in content.lower():
        errors.append("Missing <html> tag")
    if '<body' not in content.lower():
        errors.append("Missing <body> tag")
    if '</html>' not in content.lower():
        errors.append("Missing closing </html> tag")
    
    # Check for Chart.js presence
    if 'chart.js' not in content.lower():
        errors.append("Chart.js not loaded")
    
    return errors


def extract_chart_data(html_path):
    """Extract chart datasets from HTML for validation."""
    with open(html_path, 'r') as f:
        content = f.read()
    
    charts = {}
    
    # Find all Chart.js instantiations
    chart_pattern = r"new Chart\(document\.getElementById\('([^']+)'\)"
    chart_ids = re.findall(chart_pattern, content)
    
    for chart_id in chart_ids:
        # Try to find the data array for this chart
        # Look for patterns like: data: [1, 2, 3, ...]
        data_pattern = rf"getElementById\('{chart_id}'.*?data:\s*(\[[^\]]+\])"
        match = re.search(data_pattern, content, re.DOTALL)
        if match:
            try:
                # Extract just the array part
                data_str = match.group(1)
                # Simple validation: check it's not empty
                charts[chart_id] = {
                    'has_data': len(data_str.strip()) > 2,  # More than just []
                    'data_length': len(data_str.split(','))
                }
            except Exception:
                charts[chart_id] = {'has_data': False, 'data_length': 0}
    
    return charts


def validate_monthly_summary(html_path, baseline=None):
    """Validate the main monthly summary HTML report."""
    errors = []
    
    expected_sections = [
        'High-Level Pages',
        'Content Pages (Organism, Assembly, Workflow)',
        'Organism Pages by Community',
        'Assembly Pages by Community',
        'Workflow Pages by Community',
        'Workflow Pages by Category',
        'Demographics & Technology'
    ]
    
    # Structure validation
    struct_errors = validate_html_structure(html_path, expected_sections)
    errors.extend(struct_errors)
    
    # Extract chart data - use simpler approach since Chart.js is in script tags
    with open(html_path, 'r') as f:
        content = f.read()
    
    # Count Chart.js instantiations
    chart_count = content.count("new Chart(document.getElementById")
    charts = {'total_charts': chart_count}
    
    # Validate minimum chart count (we expect at least 15 charts)
    if chart_count < 15:
        errors.append(f"Too few charts found: {chart_count} (expected at least 15)")
    
    # Check for community presence (content already loaded above)
    
    expected_communities = ['Viruses', 'Bacteria', 'Fungi', 'Protists', 'Vectors', 'Hosts', 'Helminths']
    for community in expected_communities:
        if community not in content:
            errors.append(f"Community not found in output: {community}")
    
    # Baseline comparison if provided
    if baseline:
        baseline_charts = baseline.get('charts', {})
        for chart_id in baseline_charts:
            if chart_id not in charts:
                errors.append(f"Chart missing compared to baseline: {chart_id}")
    
    return {
        'errors': errors,
        'charts': charts,
        'chart_count': len(charts)
    }


def validate_analysis_html(html_path, baseline=None):
    """Validate per-month analysis HTML reports."""
    errors = []
    
    # Determine if organism or workflow analysis
    is_organism = 'organism-analysis' in str(html_path)
    is_workflow = 'workflow-analysis' in str(html_path)
    
    if is_organism:
        expected_sections = [
            'Organism & Pathogen Page Analysis',
            'High-Level Navigation Pages',
            'Top Organism Pages by Community',
            'Top Assembly Pages by Community'
        ]
    elif is_workflow:
        expected_sections = [
            'Workflow Configuration Page Analysis',
            'Visitors & Pageviews by Workflow Type',
            'Workflow-Organism Network'
        ]
    else:
        errors.append(f"Unknown analysis type: {html_path.name}")
        return {'errors': errors}
    
    # Structure validation
    struct_errors = validate_html_structure(html_path, expected_sections)
    errors.extend(struct_errors)
    
    # Extract chart data
    charts = extract_chart_data(html_path)
    
    if len(charts) == 0:
        errors.append("No charts found in analysis HTML")
    
    return {
        'errors': errors,
        'charts': charts,
        'chart_count': len(charts)
    }


def scan_output_directory(output_dir):
    """Scan output directory and categorize files."""
    output_path = Path(output_dir)
    
    files = {
        'monthly_summary': None,
        'organism_analysis': [],
        'workflow_analysis': []
    }
    
    # Find monthly summary
    summary_path = output_path / 'monthly_summary.html'
    if summary_path.exists():
        files['monthly_summary'] = summary_path
    
    # Find per-month analysis files
    fetched_dir = output_path / 'fetched'
    if fetched_dir.exists():
        for html_file in fetched_dir.glob('*-organism-analysis.html'):
            files['organism_analysis'].append(html_file)
        for html_file in fetched_dir.glob('*-workflow-analysis.html'):
            files['workflow_analysis'].append(html_file)
    
    return files


def create_baseline(output_dir, baseline_dir):
    """Create baseline snapshots of current outputs."""
    baseline_path = Path(baseline_dir)
    baseline_path.mkdir(parents=True, exist_ok=True)
    
    files = scan_output_directory(output_dir)
    
    baseline_data = {
        'created_at': datetime.now().isoformat(),
        'monthly_summary': None,
        'organism_analysis': [],
        'workflow_analysis': []
    }
    
    # Capture monthly summary baseline
    if files['monthly_summary']:
        result = validate_monthly_summary(files['monthly_summary'])
        baseline_data['monthly_summary'] = {
            'chart_count': result['chart_count'],
            'charts': list(result['charts'].keys())
        }
    
    # Capture organism analysis baseline
    for html_path in files['organism_analysis']:
        result = validate_analysis_html(html_path)
        baseline_data['organism_analysis'].append({
            'filename': html_path.name,
            'chart_count': result['chart_count'],
            'charts': list(result['charts'].keys())
        })
    
    # Capture workflow analysis baseline
    for html_path in files['workflow_analysis']:
        result = validate_analysis_html(html_path)
        baseline_data['workflow_analysis'].append({
            'filename': html_path.name,
            'chart_count': result['chart_count'],
            'charts': list(result['charts'].keys())
        })
    
    # Save baseline
    baseline_file = baseline_path / 'baseline.json'
    with open(baseline_file, 'w') as f:
        json.dump(baseline_data, f, indent=2)
    
    print(f"✓ Baseline created: {baseline_file}")
    print(f"  - Monthly summary: {baseline_data['monthly_summary']['chart_count'] if baseline_data['monthly_summary'] else 0} charts")
    print(f"  - Organism analysis: {len(baseline_data['organism_analysis'])} files")
    print(f"  - Workflow analysis: {len(baseline_data['workflow_analysis'])} files")
    
    return baseline_data


def load_baseline(baseline_dir):
    """Load baseline data if it exists."""
    baseline_file = Path(baseline_dir) / 'baseline.json'
    if not baseline_file.exists():
        return None
    
    with open(baseline_file, 'r') as f:
        return json.load(f)


def run_validation(output_dir, baseline_dir, verbose=False):
    """Run validation on all outputs."""
    files = scan_output_directory(output_dir)
    baseline = load_baseline(baseline_dir)
    
    all_errors = []
    
    print("=" * 60)
    print("BRC Analytics Output Validation")
    print("=" * 60)
    
    # Validate monthly summary
    print("\n📊 Monthly Summary Report")
    if files['monthly_summary']:
        result = validate_monthly_summary(
            files['monthly_summary'],
            baseline.get('monthly_summary') if baseline else None
        )
        if result['errors']:
            print(f"  ❌ {len(result['errors'])} errors found:")
            for error in result['errors']:
                print(f"     - {error}")
            all_errors.extend(result['errors'])
        else:
            print(f"  ✓ Valid ({result['chart_count']} charts)")
        
        if verbose and result['charts']:
            print(f"  Charts: {', '.join(result['charts'].keys())}")
    else:
        error = "Monthly summary HTML not found"
        print(f"  ❌ {error}")
        all_errors.append(error)
    
    # Validate organism analysis
    print(f"\n🧬 Organism Analysis Reports ({len(files['organism_analysis'])} files)")
    for html_path in sorted(files['organism_analysis']):
        result = validate_analysis_html(html_path)
        if result['errors']:
            print(f"  ❌ {html_path.name}: {len(result['errors'])} errors")
            if verbose:
                for error in result['errors']:
                    print(f"     - {error}")
            all_errors.extend(result['errors'])
        else:
            print(f"  ✓ {html_path.name} ({result['chart_count']} charts)")
    
    # Validate workflow analysis
    print(f"\n⚙️  Workflow Analysis Reports ({len(files['workflow_analysis'])} files)")
    for html_path in sorted(files['workflow_analysis']):
        result = validate_analysis_html(html_path)
        if result['errors']:
            print(f"  ❌ {html_path.name}: {len(result['errors'])} errors")
            if verbose:
                for error in result['errors']:
                    print(f"     - {error}")
            all_errors.extend(result['errors'])
        else:
            print(f"  ✓ {html_path.name} ({result['chart_count']} charts)")
    
    # Summary
    print("\n" + "=" * 60)
    if all_errors:
        print(f"❌ Validation FAILED: {len(all_errors)} total errors")
        return False
    else:
        print("✓ All validations PASSED")
        return True


def main():
    parser = argparse.ArgumentParser(description="Validate BRC analytics outputs")
    parser.add_argument(
        '--output-dir',
        default='output',
        help="Output directory to validate (default: output)"
    )
    parser.add_argument(
        '--baseline-dir',
        default='tests/baselines',
        help="Baseline directory (default: tests/baselines)"
    )
    parser.add_argument(
        '--create-baseline',
        action='store_true',
        help="Create baseline snapshots from current outputs"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help="Show detailed output"
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    output_dir = project_dir / args.output_dir
    baseline_dir = project_dir / args.baseline_dir
    
    if args.create_baseline:
        create_baseline(output_dir, baseline_dir)
        return 0
    
    success = run_validation(output_dir, baseline_dir, args.verbose)
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
