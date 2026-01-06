#!/usr/bin/env python3
"""
Pre-fetch taxonomy data for all organisms and assemblies and cache it.

This script scans all .tab data files, extracts unique tax IDs and assembly IDs,
fetches their taxonomy information from NCBI, and saves it to a versioned cache.

Usage:
    python fetch_taxonomy.py                    # Fetch missing, use latest cache
    python fetch_taxonomy.py --force            # Re-fetch all (create new version)
    python fetch_taxonomy.py --cache-version X  # Use specific cache version
    python fetch_taxonomy.py --data-dir PATH    # Custom data directory
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def get_cache_dir(base_dir=None):
    """Get or create the cache directory."""
    if base_dir:
        cache_dir = Path(base_dir)
    else:
        script_dir = Path(__file__).parent
        cache_dir = script_dir.parent / '.taxonomy_cache'
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_latest_cache_path(cache_dir):
    """Get the path to the latest cache file."""
    latest_link = cache_dir / 'latest.json'
    if latest_link.exists():
        if latest_link.is_symlink():
            return latest_link.resolve()
        else:
            return latest_link
    return None


def load_cache(cache_path):
    """Load an existing cache file."""
    if not cache_path or not cache_path.exists():
        return {
            'version': None,
            'created': None,
            'source_data_hash': None,
            'taxonomy': {},
            'assembly': {}
        }
    
    with open(cache_path, 'r') as f:
        data = json.load(f)
    
    # Handle old format (flat taxonomy/assembly dicts)
    if 'version' not in data:
        return {
            'version': 'legacy',
            'created': None,
            'source_data_hash': None,
            'taxonomy': data.get('taxonomy', {}),
            'assembly': data.get('assembly', {})
        }
    
    return data


def scan_data_files(data_dir):
    """Scan all .tab files and extract unique tax IDs and assembly IDs."""
    data_path = Path(data_dir)
    
    tax_ids = set()
    assembly_ids = set()
    
    # Scan top-pages files
    for tab_file in data_path.glob('top-pages-*.tab'):
        with open(tab_file, 'r') as f:
            next(f)  # Skip header
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) < 1:
                    continue
                
                url = parts[0]
                
                # Extract tax IDs from organism URLs
                if match := re.match(r'^/data/organisms/(\d+)$', url):
                    tax_ids.add(match.group(1))
                
                # Extract assembly IDs from assembly URLs
                if match := re.match(r'^/data/assemblies/([^/]+)', url):
                    assembly_ids.add(match.group(1))
    
    return sorted(tax_ids), sorted(assembly_ids)


def compute_source_hash(tax_ids, assembly_ids):
    """Compute a hash of the source data (tax IDs + assembly IDs)."""
    combined = ','.join(tax_ids) + '|' + ','.join(assembly_ids)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def fetch_taxonomy_lineage(tax_id, verbose=False):
    """Fetch taxonomy lineage from NCBI eutils."""
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
        
        if verbose:
            print(f"  ✓ {tax_id}: {name}")
        
        return {
            'name': name,
            'lineage': lineage,
            'fetched_at': datetime.now().isoformat()
        }
    except Exception as e:
        if verbose:
            print(f"  ✗ {tax_id}: Error - {e}")
        return {
            'name': 'Unknown',
            'lineage': 'Unknown',
            'fetched_at': datetime.now().isoformat(),
            'error': str(e)
        }


def fetch_assembly_taxonomy(assembly_id, verbose=False):
    """Fetch taxonomy info for an assembly from NCBI Datasets API."""
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
            
            if verbose:
                print(f"  ✓ {assembly_id}: {name} (tax_id: {tax_id})")
            
            return {
                'tax_id': tax_id,
                'name': name,
                'lineage': 'Unknown',  # Will be filled from tax_id lookup
                'fetched_at': datetime.now().isoformat()
            }
        else:
            if verbose:
                print(f"  ✗ {assembly_id}: No data found")
            return {
                'tax_id': None,
                'name': 'Unknown',
                'lineage': 'Unknown',
                'fetched_at': datetime.now().isoformat()
            }
    except Exception as e:
        if verbose:
            print(f"  ✗ {assembly_id}: Error - {e}")
        return {
            'tax_id': None,
            'name': 'Unknown',
            'lineage': 'Unknown',
            'fetched_at': datetime.now().isoformat(),
            'error': str(e)
        }


def fill_assembly_lineages(cache_data):
    """Fill in lineage data for assemblies from their tax_id lookups."""
    for assembly_id, asm_data in cache_data['assembly'].items():
        tax_id = asm_data.get('tax_id')
        if tax_id and tax_id in cache_data['taxonomy']:
            asm_data['lineage'] = cache_data['taxonomy'][tax_id]['lineage']


def save_cache(cache_data, cache_dir, version=None):
    """Save cache to a versioned file and update latest symlink."""
    if version is None:
        version = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    cache_data['version'] = version
    cache_data['created'] = datetime.now().isoformat()
    
    cache_file = cache_dir / f'cache_{version}.json'
    
    with open(cache_file, 'w') as f:
        json.dump(cache_data, f, indent=2)
    
    # Update latest symlink
    latest_link = cache_dir / 'latest.json'
    if latest_link.exists():
        latest_link.unlink()
    
    try:
        latest_link.symlink_to(cache_file.name)
    except OSError:
        # Windows doesn't always support symlinks, just copy
        import shutil
        shutil.copy(cache_file, latest_link)
    
    return cache_file


def main():
    parser = argparse.ArgumentParser(description="Pre-fetch and cache taxonomy data")
    parser.add_argument(
        '--data-dir',
        default='data/fetched',
        help="Data directory to scan (default: data/fetched)"
    )
    parser.add_argument(
        '--cache-dir',
        help="Cache directory (default: .taxonomy_cache/)"
    )
    parser.add_argument(
        '--cache-version',
        help="Use specific cache version as base"
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help="Create new snapshot even if all IDs exist in current cache"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help="Show detailed progress"
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    data_dir = project_dir / args.data_dir
    cache_dir = get_cache_dir(args.cache_dir)
    
    print("=" * 60)
    print("BRC Taxonomy Data Fetcher")
    print("=" * 60)
    
    # Scan data files
    print(f"\n📂 Scanning data files in {data_dir}...")
    tax_ids, assembly_ids = scan_data_files(data_dir)
    print(f"  Found {len(tax_ids)} unique tax IDs")
    print(f"  Found {len(assembly_ids)} unique assembly IDs")
    
    source_hash = compute_source_hash(tax_ids, assembly_ids)
    
    # Load existing cache
    if args.cache_version:
        cache_path = cache_dir / f'cache_{args.cache_version}.json'
        print(f"\n📦 Loading cache version: {args.cache_version}")
    else:
        cache_path = get_latest_cache_path(cache_dir)
        if cache_path:
            print(f"\n📦 Loading latest cache: {cache_path.name}")
        else:
            print("\n📦 No existing cache found, creating new one")
    
    cache_data = load_cache(cache_path)
    cache_data['source_data_hash'] = source_hash
    
    if cache_data.get('version'):
        print(f"  Cache version: {cache_data['version']}")
        print(f"  Cached taxonomy entries: {len(cache_data['taxonomy'])}")
        print(f"  Cached assembly entries: {len(cache_data['assembly'])}")
    
    # Identify missing entries
    missing_tax_ids = [tid for tid in tax_ids if tid not in cache_data['taxonomy']]
    missing_assembly_ids = [aid for aid in assembly_ids if aid not in cache_data['assembly']]
    
    print(f"\n🔍 Analysis:")
    print(f"  Tax IDs needed: {len(tax_ids)}")
    print(f"  Assembly IDs needed: {len(assembly_ids)}")
    print(f"  Tax IDs missing from cache: {len(missing_tax_ids)}")
    print(f"  Assembly IDs missing from cache: {len(missing_assembly_ids)}")
    
    # Decide whether to create new snapshot
    needs_new_snapshot = args.force_refresh or missing_tax_ids or missing_assembly_ids
    
    if not needs_new_snapshot:
        print("\n✓ Current snapshot has all required IDs")
        print(f"  Using existing cache: {cache_path}")
        print("  (Use --force-refresh to create new snapshot anyway)")
        return 0
    
    if args.force_refresh:
        print("\n🔄 Force refresh: creating new snapshot with all IDs")
        # Re-fetch everything
        missing_tax_ids = list(tax_ids)
        missing_assembly_ids = list(assembly_ids)
        cache_data = {
            'version': None,
            'created': None,
            'source_data_hash': source_hash,
            'taxonomy': {},
            'assembly': {}
        }
    else:
        print(f"\n🆕 Creating new snapshot (found {len(missing_tax_ids) + len(missing_assembly_ids)} new IDs)")
    
    # Fetch missing taxonomy data
    if missing_tax_ids:
        print(f"\n🧬 Fetching taxonomy data for {len(missing_tax_ids)} tax IDs...")
        for i, tax_id in enumerate(missing_tax_ids, 1):
            if args.verbose or i % 10 == 0 or i == len(missing_tax_ids):
                print(f"  [{i}/{len(missing_tax_ids)}] Tax ID {tax_id}...")
            
            cache_data['taxonomy'][tax_id] = fetch_taxonomy_lineage(tax_id, args.verbose)
            time.sleep(0.35)  # Rate limiting
    
    # Fetch missing assembly data
    if missing_assembly_ids:
        print(f"\n🔬 Fetching assembly data for {len(missing_assembly_ids)} assemblies...")
        for i, assembly_id in enumerate(missing_assembly_ids, 1):
            if args.verbose or i % 10 == 0 or i == len(missing_assembly_ids):
                print(f"  [{i}/{len(missing_assembly_ids)}] Assembly {assembly_id}...")
            
            cache_data['assembly'][assembly_id] = fetch_assembly_taxonomy(assembly_id, args.verbose)
            time.sleep(0.35)  # Rate limiting
    
    # Fill in lineages for assemblies from their tax_id lookups
    print("\n🔗 Linking assembly lineages from taxonomy data...")
    fill_assembly_lineages(cache_data)
    
    # Save new snapshot
    print("\n💾 Saving new snapshot...")
    cache_file = save_cache(cache_data, cache_dir)
    print(f"  ✓ Saved to: {cache_file}")
    print(f"  ✓ Updated: {cache_dir / 'latest.json'}")
    
    print("\n" + "=" * 60)
    print("✓ New snapshot created")
    print(f"  Total taxonomy entries: {len(cache_data['taxonomy'])}")
    print(f"  Total assembly entries: {len(cache_data['assembly'])}")
    print(f"  Snapshot: {cache_file.name}")
    print("=" * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
