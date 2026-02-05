#!/usr/bin/env python3
"""
Generate a 2025 year-in-review dashboard for stakeholder storytelling.

This script re-uses the Plausible monthly exports to build a single-page,
manually curated HTML report (not part of CI). It focuses on:
- Who visited in 2025 (audience, countries, devices)
- What they explored (organisms, workflows, communities)
- How engagement changed month-to-month
- A proxy for "analyses completed" via workflow activity
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from generate_monthly_summary_html import (
    parse_data_file,
    parse_demographics_file,
    get_month_files,
    format_month,
    WORKFLOW_CATEGORIES_ORDER,
    classify_workflow_category,
)
from taxonomy_cache import load_cache, get_community, get_organism_name

REPORT_YEAR = 2025
_ENV_LOADED = False


def ensure_env_loaded():
    """Load environment variables from .env once."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as env_file:
            for line in env_file:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())
    _ENV_LOADED = True


def get_api_config():
    """Return Plausible API configuration if available."""
    ensure_env_loaded()
    base_url = os.environ.get("PLAUSIBLE_API_BASE_URL", "https://plausible.galaxyproject.eu").rstrip("/")
    api_key = os.environ.get("PLAUSIBLE_API_KEY")
    site_id = os.environ.get("PLAUSIBLE_SITE_ID")

    if not api_key or api_key == "your-api-key-here":
        raise RuntimeError("PLAUSIBLE_API_KEY missing; cannot fetch deduplicated stats.")
    if not site_id or site_id in ("your-site-domain-here", "example.com"):
        raise RuntimeError("PLAUSIBLE_SITE_ID missing; cannot fetch deduplicated stats.")
    if not base_url.startswith(("http://", "https://")):
        raise RuntimeError("PLAUSIBLE_API_BASE_URL must include scheme (http/https).")

    return base_url, api_key, site_id


def metric_value(obj, default=0):
    """Normalize Plausible metric payloads into ints."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return metric_value(obj.get("value"), default=default)
    if isinstance(obj, (int, float)):
        return obj
    try:
        return float(obj)
    except (TypeError, ValueError):
        return default


def plausible_api_get(path, params, base_url, api_key):
    """Perform a GET request against Plausible API and return parsed JSON."""
    query = urllib.parse.urlencode(params, doseq=True)
    url = f"{base_url}{path}?{query}"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Plausible API request failed ({exc.code}): {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Plausible API connection error: {exc.reason}") from exc


def format_date_range(year, month):
    """Return ISO start/end strings for a given month."""
    start = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    end = next_month - timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_deduplicated_overview(year):
    """Fetch deduplicated totals, monthly visitors, and country breakdown via Plausible API v1."""
    base_url, api_key, site_id = get_api_config()
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # Year totals
    total_resp = plausible_api_get(
        "/api/v1/stats/aggregate",
        {
            "site_id": site_id,
            "metrics": "visitors,pageviews",
            "period": "custom",
            "date": f"{start_date},{end_date}",
        },
        base_url,
        api_key,
    )
    total_results = total_resp.get("results", {})
    totals = {
        "visitors": metric_value(total_results.get("visitors")),
        "pageviews": metric_value(total_results.get("pageviews")),
    }

    # Monthly visitors (deduplicated) by calling aggregate for each month
    monthly_visitors = []
    for month in range(1, 13):
        month_start, month_end = format_date_range(year, month)
        month_resp = plausible_api_get(
            "/api/v1/stats/aggregate",
            {
                "site_id": site_id,
                "metrics": "visitors",
                "period": "custom",
                "date": f"{month_start},{month_end}",
            },
            base_url,
            api_key,
        )
        month_visitors = metric_value(month_resp.get("results", {}).get("visitors"))
        monthly_visitors.append({
            "label": format_month(year, month),
            "visitors": month_visitors,
        })

    # Country distribution (deduplicated)
    country_resp = plausible_api_get(
        "/api/v1/stats/breakdown",
        {
            "site_id": site_id,
            "property": "visit:country",
            "metrics": "visitors",
            "period": "custom",
            "date": f"{start_date},{end_date}",
            "limit": 8,
        },
        base_url,
        api_key,
    )
    countries = []
    for row in country_resp.get("results", []):
        country_name = row.get("country") or row.get("value") or "Unknown"
        countries.append((country_name, metric_value(row.get("visitors"))))

    return {
        "totals": totals,
        "monthly": monthly_visitors,
        "countries": countries,
    }


def classify_community(lineage):
    """Wrapper so we can keep logic self-contained."""
    return get_community(lineage)


def humanize_number(value):
    if value is None:
        return "–"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,}"


def format_workflow_display(raw_name):
    """Clean up workflow names for presentation."""
    slug = raw_name.strip().replace('/', ' ').replace('-', ' ')
    tokens = slug.split()

    prefix_tokens = {'github', 'com', 'iwc', 'workflows'}
    while tokens and tokens[0].lower() in prefix_tokens:
        tokens.pop(0)

    if 'versions' in (token.lower() for token in tokens):
        for i, token in enumerate(tokens):
            if token.lower() == 'versions':
                tokens = tokens[:i]
                break

    cleaned_words = ' '.join(tokens).replace('-', ' ')
    return ' '.join(word.capitalize() for word in cleaned_words.split())


def compute_percent_change(current, previous):
    """Return percent change from previous to current."""
    if previous in (None, 0):
        return None
    try:
        return ((current - previous) / previous) * 100
    except ZeroDivisionError:
        return None


def format_change_inline(pct):
    """Return '(+X%)' style string or empty if pct not available."""
    if pct is None:
        return ""
    sign = "+" if pct >= 0 else ""
    return f"({sign}{pct:.1f}%)"


def format_change_label(pct, comparison_year):
    """Return 'vs {comparison_year}: +X%' style text."""
    if pct is None:
        return f"vs {comparison_year}: n/a"
    sign = "+" if pct >= 0 else ""
    return f"vs {comparison_year}: {sign}{pct:.1f}%"


def format_baseline_value(value, comparison_year):
    """Return '{year}: count' string for baseline contextualization."""
    if comparison_year is None:
        return "Baseline: n/a"
    if value is None:
        value = 0
    try:
        value_int = int(round(value))
    except (TypeError, ValueError):
        value_int = 0
    return f"{comparison_year}: {value_int:,}"


def load_monthly_year(target_year, data_dir, taxonomy_cache, assembly_cache):
    """Collect monthly stats for a given year."""
    month_files = get_month_files(data_dir)
    monthly_entries = []

    if not month_files:
        raise RuntimeError("No monthly data found in data/fetched.")

    for year, month, filepath in month_files:
        if year != target_year:
            continue

        stats = parse_data_file(filepath)
        date_range_part = filepath.name.replace('top-pages-', '').replace('.tab', '')

        # Demographics
        demo_data = {}
        for demo_type in ['countries', 'devices', 'browsers', 'sources']:
            demo_file = data_dir / f"demographics-{demo_type}-{date_range_part}.tab"
            demo_data[demo_type] = parse_demographics_file(demo_file)

        # Compute month totals + collections
        month_visitors = 0
        month_pageviews = 0

        def accumulate_dict(dictionary):
            nonlocal month_visitors, month_pageviews
            for values in dictionary.values():
                month_visitors += values.get('visitors', 0)
                month_pageviews += values.get('pageviews', 0)

        def accumulate_list(records, visitor_index, pageview_index):
            nonlocal month_visitors, month_pageviews
            for rec in records:
                month_visitors += rec[visitor_index]
                month_pageviews += rec[pageview_index]

        accumulate_dict(stats['high_level'])
        accumulate_list(stats['organism_pages'], 1, 2)
        accumulate_list(stats['assembly_pages'], 1, 2)
        accumulate_list(stats['workflow_pages'], 2, 3)
        accumulate_list(stats['priority_pathogen_pages'], 1, 2)
        month_visitors += stats['learn_pages']['visitors']
        month_pageviews += stats['learn_pages']['pageviews']

        monthly_entries.append({
            'label': format_month(year, month),
            'year': year,
            'month': month,
            'filepath': filepath,
            'stats': stats,
            'demographics': demo_data,
            'totals': {
                'visitors': month_visitors,
                'pageviews': month_pageviews,
                'organism_pages': len(stats['organism_pages']),
                'assembly_pages': len(stats['assembly_pages']),
                'workflow_pages': len(stats['workflow_pages']),
            }
        })

    if not monthly_entries:
        raise RuntimeError(f"No {target_year} monthly exports found. Fetch {target_year} data first.")

    monthly_entries.sort(key=lambda x: x['month'])
    return monthly_entries


def aggregate_year(monthly_entries, taxonomy_cache, assembly_cache):
    """Aggregate insights for a set of monthly entries."""
    totals = defaultdict(int)
    demographics = {
        'countries': defaultdict(int),
        'devices': defaultdict(int),
        'browsers': defaultdict(int),
        'sources': defaultdict(int),
    }
    organisms = defaultdict(lambda: {'visitors': 0, 'pageviews': 0})
    assemblies = defaultdict(lambda: {'visitors': 0, 'pageviews': 0})
    workflows = defaultdict(lambda: {'visitors': 0, 'pageviews': 0})
    workflow_categories = defaultdict(lambda: {'visitors': 0, 'pageviews': 0})
    communities = defaultdict(lambda: {'visitors': 0, 'workflows': 0, 'organisms': 0})

    monthly_trend = []

    for month in monthly_entries:
        totals['visitors'] += month['totals']['visitors']
        totals['pageviews'] += month['totals']['pageviews']
        totals['organism_pages'] += month['totals']['organism_pages']
        totals['assembly_pages'] += month['totals']['assembly_pages']
        totals['workflow_pages'] += month['totals']['workflow_pages']

        monthly_trend.append({
            'label': month['label'],
            'visitors': month['totals']['visitors'],
            'workflows': month['totals']['workflow_pages'],
        })

        for dtype, values in month['demographics'].items():
            for key, value in values.items():
                demographics[dtype][key] += value

        for tax_id, visitors, pageviews in month['stats']['organism_pages']:
            organisms[tax_id]['visitors'] += visitors
            organisms[tax_id]['pageviews'] += pageviews
            lineage = taxonomy_cache.get(str(tax_id), {}).get('lineage', 'Unknown')
            community = classify_community(lineage)
            communities[community]['visitors'] += visitors
            communities[community]['organisms'] += 1

        for assembly_id, visitors, pageviews in month['stats']['assembly_pages']:
            assemblies[assembly_id]['visitors'] += visitors
            assemblies[assembly_id]['pageviews'] += pageviews

        for assembly_id, workflow_name, visitors, pageviews in month['stats']['workflow_pages']:
            workflows[workflow_name]['visitors'] += visitors
            workflows[workflow_name]['pageviews'] += pageviews

            asm_data = assembly_cache.get(assembly_id, {})
            community = classify_community(asm_data.get('lineage', 'Unknown'))
            communities[community]['workflows'] += 1
            communities[community]['visitors'] += visitors

            category = classify_workflow_category(workflow_name)
            workflow_categories[category]['visitors'] += visitors
            workflow_categories[category]['pageviews'] += pageviews

    totals['unique_organisms'] = len(organisms)
    totals['unique_assemblies'] = len(assemblies)
    totals['unique_workflows'] = len(workflows)

    workflow_views = sum(item['pageviews'] for item in workflows.values())
    totals['workflow_views'] = workflow_views

    first_month = monthly_trend[0]
    last_month = monthly_trend[-1]
    growth_pct = None
    if first_month['visitors'] > 0:
        growth_pct = ((last_month['visitors'] - first_month['visitors']) / first_month['visitors']) * 100

    return {
        'totals': totals,
        'demographics': demographics,
        'organisms': organisms,
        'workflows': workflows,
        'workflow_categories': workflow_categories,
        'communities': communities,
        'monthly_trend': monthly_trend,
        'growth_pct': growth_pct,
        'top_month': max(monthly_trend, key=lambda m: m['visitors']),
        'start_month': first_month,
        'end_month': last_month,
    }


def prepare_highlights(aggregate, taxonomy_cache, assembly_cache):
    """Build lists for cards and tables."""
    def top_items(counter_dict, limit=5):
        return sorted(counter_dict.items(), key=lambda kv: kv[1], reverse=True)[:limit]

    top_countries = top_items(aggregate['demographics']['countries'], 8)
    top_sources = top_items(aggregate['demographics']['sources'], 4)

    sorted_organisms = sorted(
        aggregate['organisms'].items(),
        key=lambda kv: kv[1]['visitors'],
        reverse=True
    )
    organism_cards = []
    desired_organism_count = 10
    filtered_prefixes = ('acanthamoeba', '[emmonsia] crescens', 'emmonsia crescens')
    for tax_id, data in sorted_organisms:
        # Skip assembly accessions that were mistakenly captured as organisms
        if str(tax_id).startswith('GCA_') or str(tax_id).startswith('GCF_'):
            continue
        name = get_organism_name(tax_id=str(tax_id), taxonomy_cache=taxonomy_cache, assembly_cache=assembly_cache)
        display_name = name if name and name != 'Unknown' else f"Taxon {tax_id}"
        lower_name = display_name.lower()
        if lower_name.startswith('acanthamoeba') or lower_name.startswith('[emmonsia] crescens') or lower_name.startswith('emmonsia crescens'):
            continue
        organism_cards.append({
            'name': display_name,
            'visitors': data['visitors'],
            'pageviews': data['pageviews'],
        })
        if len(organism_cards) >= desired_organism_count:
            break

    top_workflows = sorted(
        aggregate['workflows'].items(),
        key=lambda kv: kv[1]['pageviews'],
        reverse=True
    )[:10]
    workflow_cards = []
    for name, data in top_workflows:
        display = format_workflow_display(name)
        workflow_cards.append({
            'name': display,
            'visitors': data['visitors'],
            'pageviews': data['pageviews'],
        })

    return {
        'countries': top_countries,
        'sources': top_sources,
        'organisms': organism_cards,
        'workflows': workflow_cards,
    }


def render_html(
    output_path,
    monthly_entries,
    aggregate,
    highlights,
    dedup_data=None,
    prev_aggregate=None,
    prev_dedup_data=None,
    prev_year=None,
):
    """Render the static HTML dashboard."""
    totals = aggregate['totals']

    # Deduplicated hero + chart data
    dedup_totals = (dedup_data or {}).get('totals', {})
    dedup_monthly = (dedup_data or {}).get('monthly', [])
    dedup_countries = (dedup_data or {}).get('countries', [])
    prev_totals = (prev_aggregate or {}).get('totals') if prev_aggregate else {}
    prev_dedup_totals = (prev_dedup_data or {}).get('totals', {})
    comparison_year = prev_year

    # Determine best month + growth using dedup visitors if available
    if dedup_monthly:
        best_month_entry = max(dedup_monthly, key=lambda m: m['visitors'])
        best_month = best_month_entry['label']
        first_month = dedup_monthly[0]
        last_month = dedup_monthly[-1]
        growth_pct = None
        if first_month['visitors'] > 0:
            growth_pct = ((last_month['visitors'] - first_month['visitors']) / first_month['visitors']) * 100
    else:
        best_month = aggregate['top_month']['label']
        first_month = aggregate['monthly_trend'][0]
        last_month = aggregate['monthly_trend'][-1]
        growth_pct = aggregate['growth_pct']

    growth_text = f"{growth_pct:.1f}%" if growth_pct is not None else "n/a"

    # Build line chart data (dedup visitors)
    chart_labels = [m['label'] for m in aggregate['monthly_trend']]
    dedup_map = {m['label']: m['visitors'] for m in dedup_monthly}
    agg_map = {m['label']: m for m in aggregate['monthly_trend']}
    visitor_series = [
        dedup_map.get(label, agg_map[label]['visitors'])
        for label in chart_labels
    ]
    line_chart = {
        'labels': chart_labels,
        'visitors': visitor_series,
    }

    # Country chart data from dedup breakdown if present, otherwise highlights
    if dedup_countries:
        country_pairs = dedup_countries
    else:
        country_pairs = highlights['countries']

    country_labels = [c for c, _ in country_pairs]
    country_values = [v for _, v in country_pairs]
    palette = ['#38bdf8', '#7dd3fc', '#60a5fa', '#c084fc', '#a855f7', '#f472b6', '#fb7185', '#facc15']
    country_colors = (palette * ((len(country_labels) // len(palette)) + 1))[:len(country_labels)]
    country_chart = {
        'labels': country_labels,
        'values': country_values,
        'colors': country_colors,
    }

    engaged_visitors = dedup_totals.get('visitors', totals['visitors'])
    engaged_prev = prev_dedup_totals.get('visitors') if comparison_year is not None else None
    visitor_pct = compute_percent_change(engaged_visitors, engaged_prev)

    organism_prev = prev_totals.get('unique_organisms') if prev_totals else None
    assembly_prev = prev_totals.get('unique_assemblies') if prev_totals else None
    workflow_views_prev = prev_totals.get('workflow_views') if prev_totals else None
    workflow_recipes_prev = prev_totals.get('unique_workflows') if prev_totals else None

    def change_strings(current, previous):
        pct = compute_percent_change(current, previous) if previous is not None else None
        inline = format_change_inline(pct) if pct is not None else ""
        baseline = format_baseline_value(previous, comparison_year)
        return inline, baseline

    org_inline, org_baseline = change_strings(totals['unique_organisms'], organism_prev)
    asm_inline, asm_baseline = change_strings(totals['unique_assemblies'], assembly_prev)
    wf_views_inline, wf_views_baseline = change_strings(totals['workflow_views'], workflow_views_prev)
    wf_recipes_inline, wf_recipes_baseline = change_strings(totals['unique_workflows'], workflow_recipes_prev)
    visitor_inline, visitor_baseline = change_strings(engaged_visitors, engaged_prev)

    metrics_cards = [
        {
            'label': 'Engaged Visitors',
            'value': humanize_number(engaged_visitors),
            'change': visitor_inline,
            'context_primary': f"Best month: {best_month}",
            'context_secondary': visitor_baseline,
        },
        {
            'label': 'Unique Organism Pages',
            'value': f"{totals['unique_organisms']:,}",
            'change': org_inline,
            'context_primary': "Distinct organisms explored",
            'context_secondary': org_baseline,
        },
        {
            'label': 'Unique Assembly Pages',
            'value': f"{totals['unique_assemblies']:,}",
            'change': asm_inline,
            'context_primary': "Distinct assemblies explored",
            'context_secondary': asm_baseline,
        },
        {
            'label': 'Analyses',
            'value': humanize_number(totals['workflow_views']),
            'change': wf_views_inline,
            'context_primary': "Lab-ready configurations opened",
            'context_secondary': wf_views_baseline,
        },
        {
            'label': 'Workflows',
            'value': f"{totals['unique_workflows']:,}",
            'change': wf_recipes_inline,
            'context_primary': "Unique workflows launched",
            'context_secondary': wf_recipes_baseline,
        },
    ]

    metrics_html = "".join(
        f"""
                <div class="metric-card">
                    <div class="metric-label">{card['label']}</div>
                    <div class="metric-value">{card['value']}{f"<span class='metric-change'>{card['change']}</span>" if card['change'] else ""}</div>
                    <div class="metric-context">{card['context_primary']}</div>
                    {f"<div class='metric-context secondary'>{card['context_secondary']}</div>" if card['context_secondary'] else ""}
                </div>
        """
        for card in metrics_cards
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>BRC Analytics · 2025 Year in Review</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg: #010914;
            --panel: rgba(4, 27, 54, 0.85);
            --panel-light: rgba(7, 38, 71, 0.9);
            --accent: #7dd3fc;
            --accent-2: #f472b6;
            --text: #e2e8f0;
            --muted: #94a3b8;
            --border: rgba(125, 211, 252, 0.2);
        }}
        * {{
            box-sizing: border-box;
        }}
        body {{
            margin: 0;
            font-family: 'Space Grotesk', 'Helvetica Neue', sans-serif;
            background: radial-gradient(circle at top, #042045 0%, #010914 60%);
            color: var(--text);
            padding: 32px;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }}
        .page {{
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 32px;
        }}
        .hero {{
            max-width: 1400px;
            margin: 0 auto 28px;
            padding: 28px;
            border-radius: 24px;
            background: linear-gradient(120deg, rgba(5,37,73,0.95), rgba(43,7,69,0.8));
            border: 1px solid var(--border);
            position: relative;
            overflow: hidden;
        }}
        .hero::after {{
            content: "";
            position: absolute;
            inset: 10px;
            border-radius: 20px;
            border: 1px solid rgba(125, 211, 252, 0.2);
            pointer-events: none;
        }}
        h1 {{
            font-size: 42px;
            margin: 0;
            letter-spacing: -0.5px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(180px, 1fr));
            gap: 16px;
            margin: 28px 0 0;
        }}
        @media (max-width: 1100px) {{
            .metrics-grid {{
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            }}
        }}
        .metric-card {{
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 20px;
            backdrop-filter: blur(20px);
        }}
        .metric-label {{
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
        }}
        .metric-value {{
            font-size: 32px;
            font-weight: 600;
            margin-top: 8px;
            display: flex;
            gap: 6px;
            align-items: baseline;
        }}
        .metric-value .metric-change {{
            font-size: 18px;
            color: var(--accent-2);
        }}
        .metric-context {{
            font-size: 14px;
            color: var(--accent);
            margin-top: 6px;
        }}
        .metric-context.secondary {{
            color: var(--muted);
        }}
        .section {{
            max-width: 1400px;
            margin: 0 auto 40px;
            width: 100%;
        }}
        .section h2 {{
            font-size: 28px;
            margin-bottom: 12px;
        }}
        .section p.lead {{
            color: var(--muted);
            margin-bottom: 24px;
        }}
        .panels {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 16px;
        }}
        .panel {{
            background: var(--panel-light);
            border-radius: 20px;
            border: 1px solid var(--border);
            padding: 16px;
            min-height: 180px;
            max-height: 360px;
        }}
        .panel.list-panel {{
            max-height: none;
            min-height: auto;
        }}
        .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            margin-bottom: 12px;
        }}
        .panel-header h3 {{
            margin: 0;
        }}
        .panel-subtext {{
            font-size: 14px;
            color: var(--muted);
        }}
        .list-item {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 10px;
            padding-bottom: 8px;
            border-bottom: 1px dashed rgba(148, 163, 184, 0.2);
        }}
        .list-item span {{
            font-weight: 500;
        }}
        canvas {{
            width: 100%;
            max-width: 100%;
            display: block;
        }}
        .ranked-list {{
            list-style: decimal;
            margin: 12px 0 0;
            padding-left: 20px;
            display: flex;
            flex-direction: column;
            gap: 6px;
        }}
        .ranked-list li {{
            padding: 10px 12px;
            border-radius: 12px;
            border: 1px solid rgba(148,163,184,0.25);
            background: rgba(124,58,237,0.08);
            font-weight: 500;
        }}
        .ranked-list li span {{
            color: var(--muted);
            font-weight: 400;
            display: block;
        }}
        .insights-row {{
            display: grid;
            grid-template-columns: minmax(320px, 1.5fr) repeat(2, minmax(260px, 1fr));
            gap: 20px;
            align-items: stretch;
        }}
        .chart-stack {{
            display: flex;
            flex-direction: column;
            gap: 16px;
        }}
        .chart-stack .panel {{
            max-height: 320px;
            overflow: hidden;
        }}
        .chart-stack .panel canvas {{
            max-height: 260px !important;
        }}
        @media (max-width: 1100px) {{
            .insights-row {{
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            }}
        }}
        footer {{
            text-align: center;
            font-size: 13px;
            color: var(--muted);
            margin-top: 24px;
        }}
        @media (max-width: 720px) {{
            body {{
                padding: 18px;
            }}
            .hero {{
                padding: 24px;
            }}
            h1 {{
                font-size: 32px;
            }}
        }}
    </style>
    <style media="print">
        @page {{
            size: letter landscape;
            margin: 0.25in;
        }}
        html, body {{
            margin: 0;
            padding: 0;
        }}
        body {{
            background: #0f172a;
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
            font-size: 11px;
        }}
        .page {{
            background: #0f172a;
            max-width: 100%;
            margin: 0;
            padding: 0;
            width: 100%;
        }}
        main.page {{
            margin: 0;
            padding: 0;
            width: 100%;
        }}
        .section {{
            max-width: 100%;
            margin: 0;
            width: 100%;
        }}
        .hero {{
            background: #1e293b;
            border: 1px solid #334155;
            box-shadow: none;
            padding: 16px 20px 20px;
            margin-bottom: 8px;
            width: 100%;
        }}
        h1 {{
            font-size: 20px;
            margin: 0 0 6px 0;
        }}
        .metrics-grid {{
            grid-template-columns: repeat(5, 1fr);
            gap: 8px;
            margin-bottom: 4px;
        }}
        .metric-card {{
            background: #1e293b;
            border: 1px solid #334155;
            box-shadow: none;
            padding: 8px;
        }}
        .metric-value {{
            font-size: 14px;
        }}
        .metric-label {{
            font-size: 9px;
        }}
        .metric-context {{
            font-size: 8px;
        }}
        .metric-context.secondary {{
            font-size: 8px;
        }}
        .panel {{
            background: #1e293b;
            border: 1px solid #334155;
            box-shadow: none;
            break-inside: avoid;
            padding: 8px;
        }}
        .panel-header {{
            margin-bottom: 12px;
        }}
        .panel-header h3 {{
            font-size: 15px;
            margin: 0;
        }}
        .panel-subtext {{
            font-size: 10px;
            margin: 0;
        }}
        .insights-row {{
            grid-template-columns: 1fr 1fr 1fr;
            gap: 8px;
            margin: 0;
        }}
        .chart-stack {{
            display: flex;
            flex-direction: column;
            justify-content: center;
            flex: 1;
            gap: 8px;
        }}
        .chart-stack .panel {{
            display: flex;
            flex-direction: column;
            min-height: 240px;
            height: auto;
            overflow: hidden;
        }}
        .chart-stack .panel > canvas {{
            margin-top: auto;
            margin-bottom: auto;
        }}
        .panel.list-panel {{
            display: flex;
            flex-direction: column;
            height: auto;
            min-height: auto;
        }}
        .panel.list-panel h3 {{
            font-size: 15px;
            margin: 0;
            flex-shrink: 0;
        }}
        .panel.list-panel .ranked-list {{
            margin-top: auto;
            margin-bottom: auto;
        }}
        canvas {{
            max-height: 150px !important;
            width: 100% !important;
            height: 150px !important;
            margin: 0;
        }}
        .ranked-list {{
            font-size: 10px;
            margin: 12px 0 0;
            padding-left: 12px;
            line-height: 1.1;
        }}
        .ranked-list li {{
            margin-bottom: 2px;
        }}
        footer {{
            font-size: 8px;
            margin-top: 8px;
        }}
    </style>
</head>
<body>
    <main class="page">
        <div class="hero">
            <h1>BRC Analytics · 2025 Year in Review</h1>
            <div class="metrics-grid">
{metrics_html}
            </div>
        </div>

        <div class="section insights-row">
            <div class="chart-stack">
                <div class="panel">
                    <div class="panel-header">
                        <h3>Engagement Trajectory</h3>
                        <span class="panel-subtext">+{growth_text} vs January</span>
                    </div>
                    <canvas id="trendChart" height="220"></canvas>
                </div>
                <div class="panel">
                    <div class="panel-header">
                        <h3>Global Reach</h3>
                        <span class="panel-subtext">Most-active countries</span>
                    </div>
                    <canvas id="countryChart" height="220"></canvas>
                </div>
            </div>
            <div class="panel list-panel">
                <h3>Top Organisms</h3>
                <ol class="ranked-list">
                    {''.join(f"<li>{org['name']}</li>" for org in highlights['organisms'])}
                </ol>
            </div>
            <div class="panel list-panel">
                <h3>Workflows in Demand</h3>
                <ol class="ranked-list">
                    {''.join(f"<li>{wf['name']}</li>" for wf in highlights['workflows'])}
                </ol>
            </div>
        </div>
    </main>

    <footer>
        Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · BRC Analytics annual report.
    </footer>

    <script>
        const trendData = {json.dumps(line_chart)};
        const countryData = {json.dumps(country_chart)};

        new Chart(document.getElementById('trendChart'), {{
            type: 'line',
            data: {{
                labels: trendData.labels,
                datasets: [
                    {{
                        label: 'Visitors',
                        data: trendData.visitors,
                        borderColor: '#7dd3fc',
                        backgroundColor: 'rgba(125, 211, 252, 0.15)',
                        tension: 0.35,
                        fill: true
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                }},
                scales: {{
                    x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: 'rgba(148,163,184,0.1)' }} }},
                    y: {{
                        ticks: {{ color: '#94a3b8' }},
                        grid: {{ color: 'rgba(148,163,184,0.1)' }},
                        title: {{ display: true, text: 'Visitors', color: '#cbd5f5' }}
                    }}
                }}
            }}
        }});

        new Chart(document.getElementById('countryChart'), {{
            type: 'pie',
            data: {{
                labels: countryData.labels,
                datasets: [{{
                    label: 'Visitors',
                    backgroundColor: countryData.colors,
                    data: countryData.values
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ position: 'right', labels: {{ color: '#cbd5f5' }} }}
                }}
            }}
        }});

    </script>
</body>
</html>
"""
    with open(output_path, 'w') as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser(description="Generate the 2025 year-in-review dashboard HTML.")
    parser.add_argument("--output", "-o", default="output/manual_reports/2025-year-in-review.html", help="Output HTML path")
    parser.add_argument("--data-dir", default="data/fetched", help="Directory containing Plausible TSV exports")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_path = Path(args.output)
    if not data_dir.exists():
        raise SystemExit(f"Data directory not found: {data_dir}")

    taxonomy_cache, assembly_cache = load_cache()
    if not taxonomy_cache:
        print("Warning: taxonomy cache is empty; organism names may show as IDs.", file=sys.stderr)

    monthly_entries = load_monthly_year(REPORT_YEAR, data_dir, taxonomy_cache, assembly_cache)
    aggregate = aggregate_year(monthly_entries, taxonomy_cache, assembly_cache)
    highlights = prepare_highlights(aggregate, taxonomy_cache, assembly_cache)

    prev_year = REPORT_YEAR - 1
    aggregate_prev = None
    prev_entries = None
    try:
        prev_entries = load_monthly_year(prev_year, data_dir, taxonomy_cache, assembly_cache)
        aggregate_prev = aggregate_year(prev_entries, taxonomy_cache, assembly_cache)
    except Exception as exc:
        print(f"Warning: Could not load {prev_year} monthly exports ({exc}). Percent deltas will be omitted.", file=sys.stderr)

    dedup_data = None
    try:
        dedup_data = fetch_deduplicated_overview(REPORT_YEAR)
    except Exception as exc:  # pragma: no cover - best effort
        print(f"Warning: Could not fetch deduplicated overview ({exc}). Falling back to aggregated TSV sums.", file=sys.stderr)

    dedup_prev = None
    if aggregate_prev:
        try:
            dedup_prev = fetch_deduplicated_overview(prev_year)
        except Exception as exc:  # pragma: no cover
            print(f"Warning: Could not fetch {prev_year} deduplicated overview ({exc}).", file=sys.stderr)

    render_html(
        output_path,
        monthly_entries,
        aggregate,
        highlights,
        dedup_data=dedup_data,
        prev_aggregate=aggregate_prev,
        prev_dedup_data=dedup_prev,
        prev_year=prev_year,
    )
    print(f"Report written to: {output_path}")


if __name__ == "__main__":
    main()
