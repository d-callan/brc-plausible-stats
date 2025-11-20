# brc-plausible-stats

Python scripts for analyzing web usage data exported from Plausible Analytics. These scripts help summarize visitor patterns for organism pages, priority pathogen pages, assembly pages, and workflow configurations.

## Directory Structure

```
brc-plausible-stats/
├── scripts/          # Main analysis scripts
│   ├── analyze_organisms.py   # Analyze organism/pathogen/assembly pages
│   ├── analyze_workflows.py   # Analyze workflow configuration pages
│   └── run_analysis.py        # Run both analyses at once
├── utils/            # Data cleaning utilities
│   └── clean_plausible_data.py  # Clean Plausible export data
├── data/             # Input .tab files from Plausible
├── output/           # Analysis results
└── README.md
```

## Requirements

- Python 3.6+
- `curl` command-line tool (for NCBI API calls)
- Standard library only (no pip dependencies)

## Usage

### Quick Start

1. Export page data from Plausible Analytics as a tab-separated file
2. Place the file in the `data/` directory
3. Run the analysis:

```bash
cd scripts
python3 run_analysis.py ../data/your-data-file.tab
```

This will generate two output files in the `output/` directory:
- `your-data-file-organism-analysis.txt`
- `your-data-file-workflow-analysis.txt`

### Individual Scripts

**Analyze organism/pathogen/assembly pages:**
```bash
python3 scripts/analyze_organisms.py data/your-file.tab output/results.txt
```

**Analyze workflow pages:**
```bash
python3 scripts/analyze_workflows.py data/your-file.tab output/results.txt
```

### Data Cleaning

Plausible Analytics exports data with URLs and metrics on separate lines. Use the cleaning utility to convert this to proper tab-separated format:

```bash
# Clean Plausible export data
python3 utils/clean_plausible_data.py data/your-file.tab

# Or specify output file
python3 utils/clean_plausible_data.py data/your-file.tab data/your-file-cleaned.tab
```

The script handles:
- URLs and data on separate lines (standard Plausible format)
- Missing or inconsistent tab separators
- Various formatting edge cases

## Input Format

The scripts expect tab-separated files with the following columns:
```
Page url    Visitors    Pageviews    Bounce rate    Time on Page
```

Example:
```
/data/organisms/562    15    23    65%    2m 30s
/data/assemblies/GCA_000005845_2    8    12    75%    1m 45s
```

## Output

### Organism Analysis
- Overall statistics for organism, pathogen, and assembly pages
- High-level navigation page metrics
- Priority pathogen page breakdown
- Organism pages (all and filtered by assembly status)
- Assembly pages (all and filtered by workflow status)

### Workflow Analysis
- Overall workflow statistics
- Per-workflow breakdown
- Workflow-organism intersections
- Per-assembly breakdown

## Notes

- The scripts make API calls to NCBI to fetch organism names
- Rate limiting is built in (3 requests per second)
- Analysis can take several minutes for large datasets
- Assembly IDs like `GCA_001008285_1` may show bias indicators if they appear early in listings
