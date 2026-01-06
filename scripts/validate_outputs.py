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

from taxonomy_cache import get_community, load_cache


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _extract_assembly_id_from_url(url):
    match = re.match(r'^/data/assemblies/([^/]+)', url)
    if not match:
        return None
    assembly_id = match.group(1)
    # Guard against query/flags that sometimes end up glued to the assembly id.
    assembly_id = assembly_id.split('?', 1)[0]
    assembly_id = assembly_id.split('&', 1)[0]
    return assembly_id


def _is_workflow_page(url):
    # Support both historical patterns.
    return ('workflow' in url) or ('/workflows' in url)


def _parse_tab_rows(tab_path):
    with open(tab_path, 'r') as f:
        header = f.readline().strip().split('\t')
        header_map = {col.strip().lower(): i for i, col in enumerate(header)}

        url_idx = header_map.get('page')
        if url_idx is None:
            url_idx = header_map.get('url', 0)

        visitors_idx = header_map.get('visitors')
        pageviews_idx = header_map.get('pageviews')

        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if url_idx >= len(parts):
                continue
            url = parts[url_idx]
            visitors = _safe_float(parts[visitors_idx], 0.0) if visitors_idx is not None and visitors_idx < len(parts) else 0.0
            pageviews = _safe_float(parts[pageviews_idx], 0.0) if pageviews_idx is not None and pageviews_idx < len(parts) else 0.0
            yield url, visitors, pageviews


def validate_other_share(data_dir, cache_path, max_other_pct=0.05):
    """Validate that the share of 'Other' community pages stays below a threshold.

    We compute share using visitor counts (fallback to pageviews if visitors are missing).
    """
    cache_path = Path(cache_path)
    taxonomy = {}
    assembly = {}

    # taxonomy_cache.load_cache() expects a cache *directory* (or None), not a json file path.
    # Our CLI uses a json file path by default, so handle that explicitly.
    if cache_path.exists() and cache_path.is_file() and cache_path.suffix.lower() == '.json':
        with open(cache_path, 'r') as f:
            cache_data = json.load(f)
        taxonomy = cache_data.get('taxonomy', {})
        assembly = cache_data.get('assembly', {})
    else:
        cache = load_cache(str(cache_path))
        if isinstance(cache, tuple) and len(cache) == 2:
            taxonomy, assembly = cache
        else:
            taxonomy = getattr(cache, 'get', lambda _k, _d=None: _d)('taxonomy', {})
            assembly = getattr(cache, 'get', lambda _k, _d=None: _d)('assembly', {})

    totals = {
        'assemblies': {'total': 0.0, 'other': 0.0},
        'workflows': {'total': 0.0, 'other': 0.0},
    }

    data_path = Path(data_dir)
    for tab_path in sorted(data_path.glob('top-pages-*.tab')):
        if 'all-time' in tab_path.name:
            continue

        for url, visitors, pageviews in _parse_tab_rows(tab_path):
            assembly_id = _extract_assembly_id_from_url(url)
            if not assembly_id:
                continue

            weight = visitors if visitors > 0 else pageviews
            if weight <= 0:
                continue

            asm = assembly.get(assembly_id, {})
            tax_id = asm.get('tax_id')
            lineage = asm.get('lineage')
            if (not lineage) or lineage == 'Unknown':
                if tax_id and str(tax_id) in taxonomy:
                    lineage = taxonomy[str(tax_id)].get('lineage')

            community = get_community(lineage)
            key = 'workflows' if _is_workflow_page(url) else 'assemblies'

            totals[key]['total'] += weight
            if community == 'Other':
                totals[key]['other'] += weight

    errors = []
    for key, values in totals.items():
        total = values['total']
        other = values['other']
        if total <= 0:
            continue
        pct = other / total
        if pct > max_other_pct:
            errors.append(f"{key}: Other share {pct:.1%} exceeds threshold {max_other_pct:.1%} (other={other:.1f}, total={total:.1f})")

    return errors


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


def run_validation(output_dir, baseline_dir, verbose=False, data_dir=None, cache_path=None, max_other_pct=0.05):
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

    # Heuristic: 'Other' community share should be low
    if data_dir and cache_path:
        print(f"\n🏷️  Community Heuristic (Other ≤ {max_other_pct:.1%})")
        other_errors = validate_other_share(data_dir, cache_path, max_other_pct=max_other_pct)
        if other_errors:
            print(f"  ❌ {len(other_errors)} issues found:")
            for error in other_errors:
                print(f"     - {error}")
            all_errors.extend(other_errors)
        else:
            print("  ✓ Valid")
    
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

    parser.add_argument(
        '--data-dir',
        default='data/fetched',
        help="Data directory containing top-pages-*.tab files (default: data/fetched)"
    )
    parser.add_argument(
        '--taxonomy-cache',
        default='.taxonomy_cache/latest.json',
        help="Path to taxonomy cache JSON (default: .taxonomy_cache/latest.json)"
    )
    parser.add_argument(
        '--max-other-pct',
        type=float,
        default=0.05,
        help="Fail validation if 'Other' share exceeds this threshold (default: 0.05)"
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    output_dir = project_dir / args.output_dir
    baseline_dir = project_dir / args.baseline_dir
    
    if args.create_baseline:
        create_baseline(output_dir, baseline_dir)
        return 0
    
    data_dir = project_dir / args.data_dir
    cache_path = project_dir / args.taxonomy_cache
    success = run_validation(
        output_dir,
        baseline_dir,
        verbose=args.verbose,
        data_dir=data_dir,
        cache_path=cache_path,
        max_other_pct=args.max_other_pct,
    )
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
