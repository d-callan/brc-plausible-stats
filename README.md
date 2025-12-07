# brc-plausible-stats

Python scripts for analyzing web usage data exported from Plausible Analytics. These scripts help summarize visitor patterns for organism pages, priority pathogen pages, assembly pages, and workflow configurations.

## Directory Structure

```
brc-plausible-stats/
├── scripts/
│   ├── fetch_top_pages.py         # Fetch top pages from Plausible API
│   ├── fetch_monthly_reports.py   # Fetch & analyze monthly reports in batch
│   ├── generate_monthly_summary.py      # Generate text summary by community
│   ├── generate_monthly_summary_html.py # Generate HTML report with charts
│   ├── generate_analysis_html.py        # Generate HTML from analysis text files
│   ├── analyze_organisms.py       # Analyze organism/pathogen/assembly pages
│   ├── analyze_workflows.py       # Analyze workflow configuration pages
│   └── run_analysis.py            # Run both analyses at once
├── utils/
│   └── clean_plausible_data.py  # Clean manually exported Plausible data
├── data/
│   ├── fetched/      # Data fetched via API (auto-generated)
│   └── manual/       # Manually exported data files
├── output/
│   ├── fetched/      # Analysis results for API-fetched data
│   └── manual/       # Analysis results for manually exported data
├── .env.example      # Template for API configuration
└── README.md
```

## Requirements

- Python 3.6+
- `curl` command-line tool (for NCBI API calls)
- Standard library only (no pip dependencies)
- Plausible Analytics Stats API key (for automatic data fetching)

## Setup

### API Key Configuration

To automatically fetch data from Plausible Analytics, you need to configure your API key:

1. Log in to your [Plausible Analytics](https://plausible.io) account
2. Click your account name in the top-right menu and go to **Settings**
3. Go to **API Keys** in the left sidebar
4. Click **New API Key**, choose **Stats API**, and save the key (it will only be shown once)
5. Copy the example environment file and add your credentials:

```bash
cp .env.example .env
```

6. Edit `.env` and fill in your values:

```
PLAUSIBLE_API_KEY=your-actual-api-key
PLAUSIBLE_SITE_ID=your-site-domain.com
```

## Usage

### Quick Start (Automatic Data Fetching)

1. Configure your API key (see Setup above)
2. Fetch top pages data from Plausible:

```bash
# Fetch last 30 days
python3 scripts/fetch_top_pages.py --period 30d

# Or fetch a specific date range
python3 scripts/fetch_top_pages.py --start 2024-01-01 --end 2024-01-31
```

3. Run the analysis on the fetched data:

```bash
python3 scripts/run_analysis.py data/fetched/top-pages-2024-01-01-to-2024-01-31.tab
```

This will generate two output files in `output/fetched/`:
- `*-organism-analysis.txt`
- `*-workflow-analysis.txt`

### Batch Monthly Reports

To fetch and analyze data for multiple months at once:

```bash
# Fetch all months from Oct 2024 to present and run analysis
python3 scripts/fetch_monthly_reports.py

# Specify custom date range
python3 scripts/fetch_monthly_reports.py --start-month 2024-10 --end-month 2025-06

# Only fetch data, skip analysis
python3 scripts/fetch_monthly_reports.py --skip-analysis
```

This saves data to `data/fetched/` and analysis output to `output/fetched/`.

### Monthly Summary Report

Generate a comprehensive summary across all months with breakdowns by community:

```bash
python3 scripts/generate_monthly_summary.py --output output/monthly_summary.txt
```

This produces a report with:
- **High-level pages**: Home, Organisms Index, Assemblies Index, Roadmap, About, etc.
- **Content page totals**: Organism, Assembly, and Workflow page counts/visitors/pageviews
- **Community breakdowns**: Pages categorized by Viruses, Bacteria, Fungi, Protists, Vectors, Hosts, Helminths
- **Learn pages**: Featured analyses traffic

The script caches taxonomy lookups in `.taxonomy_cache.json` to speed up subsequent runs.

#### HTML Report with Charts

Generate an interactive HTML report with line charts:

```bash
python3 scripts/generate_monthly_summary_html.py --output output/monthly_summary.html
```

This produces an HTML file with Chart.js visualizations showing trends over time:
- Line charts for visitors and pageviews
- Separate charts for high-level pages, content pages, and community breakdowns
- Bar charts for community comparison (uses all-time data if available)
- Interactive tooltips and legends

For accurate all-time bar charts, first fetch all-time data:
```bash
python3 scripts/fetch_monthly_reports.py --include-all-time --skip-analysis
```

#### Individual Analysis HTML Reports

Convert monthly text analysis files to interactive HTML:

```bash
# Convert a single file
python3 scripts/generate_analysis_html.py output/fetched/top-pages-2025-05-01-to-2025-05-31-organism-analysis.txt

# Convert all analysis files in a directory
python3 scripts/generate_analysis_html.py output/fetched/
```

This generates HTML reports with:
- Summary statistics cards
- Bar charts comparing page types
- Sortable tables with links to NCBI

### Quick Start (Manual Export)

Alternatively, you can manually export data from Plausible:

1. Export page data from Plausible Analytics as a tab-separated file
2. Place the file in the `data/manual/` directory
3. Run the analysis:

```bash
python3 scripts/run_analysis.py data/manual/your-data-file.tab
```

Output will be saved to `output/manual/`.

### Fetching Data from Plausible API

The `fetch_top_pages.py` script queries the Plausible Stats API v1 to retrieve top pages:

```bash
# Fetch using preset time periods
python3 scripts/fetch_top_pages.py --period 7d      # Last 7 days
python3 scripts/fetch_top_pages.py --period 30d     # Last 30 days
python3 scripts/fetch_top_pages.py --period 6mo     # Last 6 months
python3 scripts/fetch_top_pages.py --period year    # Last year
python3 scripts/fetch_top_pages.py --period all     # All time

# Fetch specific date range
python3 scripts/fetch_top_pages.py --start 2024-01-01 --end 2024-06-30

# Specify custom output file
python3 scripts/fetch_top_pages.py --period 30d --output data/my-report.tab

# Limit number of pages (default: 10000)
python3 scripts/fetch_top_pages.py --period 30d --limit 500
```

Available period presets: `day`, `7d`, `28d`, `30d`, `91d`, `month`, `6mo`, `12mo`, `year`, `all`

### Individual Analysis Scripts

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

## Data Source

Data can be obtained in two ways:

1. **Automatic (Recommended)**: Use `fetch_top_pages.py` to pull data directly from the Plausible Stats API. This produces clean, properly formatted TSV files.

2. **Manual**: Export the "Top pages" section from Plausible for BRC Analytics and save as a `.tab` file. Use the cleaning utility to convert raw copy-paste into a consistent TSV format for analysis.

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
- Organism pages (all and filtered to show where available assembly pages were not visited)
- Assembly pages (all and filtered to show where available workflow pages were not visited)

### Workflow Analysis
- Overall workflow statistics
- Per-workflow breakdown
- Workflow-organism intersections
- Per-assembly breakdown

## GitHub Pages Deployment

The repository includes a GitHub Actions workflow that automatically builds and deploys reports to GitHub Pages.

### Setup

1. **Enable GitHub Pages** in your repository:
   - Go to **Settings** → **Pages**
   - Under "Build and deployment", set **Source** to **GitHub Actions**

2. **Add repository secrets** for the Plausible API:
   - Go to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret** and add:
     - `PLAUSIBLE_API_KEY` - Your Plausible Stats API key
     - `PLAUSIBLE_SITE_ID` - Your site domain (e.g., `brc-analytics.org`)
     - `PLAUSIBLE_API_BASE_URL` - API base URL (e.g., `https://plausible.io`)

3. **Trigger the workflow**:
   - Push to `main` branch, or
   - Go to **Actions** → **Build and Deploy to GitHub Pages** → **Run workflow**

The workflow runs automatically:
- On every push to `main`
- Weekly on Mondays at 6am UTC
- Manually via workflow dispatch

### What gets deployed

- `index.html` - Main summary report with timeline charts
- `fetched/*.html` - Per-month organism and workflow analysis reports

Click on data points in the timeline charts to navigate to the corresponding monthly report.

## Notes

- The scripts make API calls to NCBI to fetch organism names
- Rate limiting is built in (3 requests per second)
- Analysis can take several minutes for large datasets
- Assembly IDs like `GCA_001008285_1` may show bias indicators if they appear early in listings
- **"Without assembly/workflow" sections** show pages where the site offers these features but users didn't visit them during the reporting period
