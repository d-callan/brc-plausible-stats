#!/usr/bin/env python3
"""
Shared taxonomy cache module for BRC analytics.

This module provides functions to load taxonomy data from the versioned cache
and classify organisms into communities based on their lineage.

Usage:
    from taxonomy_cache import load_cache, get_community
    
    taxonomy, assembly = load_cache()
    community = get_community(lineage)
"""

import json
from pathlib import Path


# Community classification patterns
COMMUNITY_PATTERNS = {
    'Viruses': ['Viruses'],
    'Bacteria': ['Bacteria'],
    'Fungi': ['Fungi'],
    'Protists': [
        'Apicomplexa', 'Amoebozoa', 'Euglenozoa', 'Heterolobosea',
        'Diplomonadida', 'Parabasalia', 'Fornicata', 'Metamonada'
    ],
    'Vectors': [
        'Culicidae', 'Ixodidae', 'Glossinidae', 'Psychodidae',
        'Simuliidae', 'Reduviidae', 'Pulicidae', 'Muscidae'
    ],
    'Hosts': [
        'Mammalia', 'Aves', 'Amphibia', 'Reptilia', 'Actinopterygii'
    ],
    'Helminths': [
        'Nematoda', 'Platyhelminthes', 'Cestoda', 'Trematoda',
        'Secernentea', 'Chromadorea'
    ]
}


def get_cache_dir(base_dir=None):
    """Get the cache directory path."""
    if base_dir:
        return Path(base_dir)
    
    script_dir = Path(__file__).parent
    return script_dir.parent / '.taxonomy_cache'


def get_latest_cache_path(cache_dir):
    """Get the path to the latest cache file."""
    latest_link = cache_dir / 'latest.json'
    if latest_link.exists():
        if latest_link.is_symlink():
            return latest_link.resolve()
        else:
            return latest_link
    
    # Fallback: find most recent cache file
    cache_files = sorted(cache_dir.glob('cache_*.json'), reverse=True)
    if cache_files:
        return cache_files[0]
    
    return None


def load_cache(cache_dir=None, version=None):
    """
    Load taxonomy cache from disk.
    
    Args:
        cache_dir: Optional custom cache directory path
        version: Optional specific cache version to load (e.g., "2025-12-22_11-55-09")
    
    Returns:
        Tuple of (taxonomy_dict, assembly_dict) where:
        - taxonomy_dict: {tax_id: {'name': str, 'lineage': str, ...}}
        - assembly_dict: {assembly_id: {'tax_id': str, 'name': str, 'lineage': str, ...}}
    """
    cache_path_obj = get_cache_dir(cache_dir)
    
    if version:
        cache_file = cache_path_obj / f'cache_{version}.json'
    else:
        cache_file = get_latest_cache_path(cache_path_obj)
    
    # If no cache exists, return empty dicts (fetch_taxonomy.py will create one)
    if not cache_file or not cache_file.exists():
        return {}, {}
    
    # Load versioned cache
    with open(cache_file, 'r') as f:
        data = json.load(f)
    
    return data.get('taxonomy', {}), data.get('assembly', {})


def get_community(lineage):
    """
    Classify an organism into a community based on its taxonomic lineage.
    
    Args:
        lineage: Semicolon-separated taxonomic lineage string
    
    Returns:
        Community name string (e.g., 'Viruses', 'Bacteria', 'Fungi', etc.)
        Returns 'Other' if no match is found.
    """
    if not lineage or lineage == 'Unknown':
        return 'Other'
    
    lineage_lower = lineage.lower()
    
    # Check each community pattern
    for community, patterns in COMMUNITY_PATTERNS.items():
        for pattern in patterns:
            if pattern.lower() in lineage_lower:
                return community
    
    return 'Other'


def get_organism_name(tax_id=None, assembly_id=None, taxonomy_cache=None, assembly_cache=None):
    """
    Get organism name from tax_id or assembly_id.
    
    Args:
        tax_id: NCBI taxonomy ID (string)
        assembly_id: NCBI assembly ID (string)
        taxonomy_cache: Pre-loaded taxonomy cache dict (optional)
        assembly_cache: Pre-loaded assembly cache dict (optional)
    
    Returns:
        Organism name string, or 'Unknown' if not found
    """
    # Load caches if not provided
    if taxonomy_cache is None or assembly_cache is None:
        taxonomy_cache, assembly_cache = load_cache()
    
    if tax_id and tax_id in taxonomy_cache:
        return taxonomy_cache[tax_id].get('name', 'Unknown')
    
    if assembly_id and assembly_id in assembly_cache:
        return assembly_cache[assembly_id].get('name', 'Unknown')
    
    return 'Unknown'


def get_lineage(tax_id=None, assembly_id=None, taxonomy_cache=None, assembly_cache=None):
    """
    Get taxonomic lineage from tax_id or assembly_id.
    
    Args:
        tax_id: NCBI taxonomy ID (string)
        assembly_id: NCBI assembly ID (string)
        taxonomy_cache: Pre-loaded taxonomy cache dict (optional)
        assembly_cache: Pre-loaded assembly cache dict (optional)
    
    Returns:
        Lineage string, or 'Unknown' if not found
    """
    # Load caches if not provided
    if taxonomy_cache is None or assembly_cache is None:
        taxonomy_cache, assembly_cache = load_cache()
    
    if tax_id and tax_id in taxonomy_cache:
        return taxonomy_cache[tax_id].get('lineage', 'Unknown')
    
    if assembly_id and assembly_id in assembly_cache:
        return assembly_cache[assembly_id].get('lineage', 'Unknown')
    
    return 'Unknown'


def main():
    """Test the cache loading and classification."""
    print("Testing taxonomy cache module...")
    print("=" * 60)
    
    # Load cache
    print("\nLoading cache...")
    taxonomy, assembly = load_cache()
    print(f"  Loaded {len(taxonomy)} taxonomy entries")
    print(f"  Loaded {len(assembly)} assembly entries")
    
    # Test classification
    print("\nTesting classification:")
    test_cases = [
        ("Viruses; Riboviria; ...", "Viruses"),
        ("cellular organisms; Bacteria; ...", "Bacteria"),
        ("cellular organisms; Eukaryota; ... Fungi; ...", "Fungi"),
        ("... Apicomplexa; ...", "Protists"),
        ("... Culicidae; ...", "Vectors"),
        ("... Mammalia; ...", "Hosts"),
        ("... Nematoda; ...", "Helminths"),
        ("Unknown", "Other"),
    ]
    
    for lineage, expected in test_cases:
        result = get_community(lineage)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {lineage[:40]:40s} -> {result:10s} (expected: {expected})")
    
    # Show some real examples
    print("\nReal examples from cache:")
    for i, (tax_id, data) in enumerate(list(taxonomy.items())[:5]):
        name = data.get('name', 'Unknown')
        lineage = data.get('lineage', 'Unknown')
        community = get_community(lineage)
        print(f"  {tax_id}: {name} -> {community}")
    
    print("\n" + "=" * 60)


if __name__ == '__main__':
    main()
