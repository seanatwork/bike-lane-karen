# Bike Lane Karen - Austin 311 Bicycle Infrastructure Complaints Catalog

A data analysis and visualization tool for cataloging and analyzing bicycle infrastructure-related complaints from the Austin 311 system. This project processes Austin Transportation and Public Works service requests to identify trends, patterns, and hotspots in bicycle infrastructure issues reported by citizens.

## Purpose

This project aims to:
- Extract and filter bicycle infrastructure complaints from Austin's 311 service request data
- Provide interactive visualizations to analyze complaint patterns over time
- Identify the most common types of bicycle infrastructure issues
- Help city planners and cycling advocates understand infrastructure needs through data-driven insights

## Features

- **Data Processing**: Filters large 311 datasets to focus on recent bicycle-related complaints
- **Interactive Dashboard**: Streamlit-based web interface for data exploration
- **Time Series Analysis**: Visualizes complaint trends over time
- **Category Analysis**: Shows most common service request types and descriptions
- **Method Tracking**: Analyzes how complaints are reported (phone, web, mobile app, etc.)

## Project Structure

```
├── dashboard.py                    # Main Streamlit dashboard application
├── filter_csv.py                   # Script to filter raw 311 data to last 365 days
├── filter_csv.ps1                  # PowerShell version of the filtering script
├── scrape_bicycle_complaints.py    # Original bicycle-specific scraper (legacy)
├── search_311_categories.py        # NEW: General 311 category search tool
├── test_category_search.py         # Test script for category search functionality
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── 311austin.htm                   # Austin 311 website HTML for category extraction
├── 311categories.txt               # List of available 311 categories
└── data/
    ├── 311_Service_Requests_-_Austin_Transportation_and_Public_Works_20260322.csv  # Raw dataset (gitignored)
    └── 311_Service_Requests_Last_365_Days.csv                                      # Filtered dataset
```

## Setup and Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "bike lane karen"
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # or
   source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   # Install the project and its entry points
   pip install -e .
   ```



## Usage

### Running the Dashboard

Start the Streamlit dashboard:

```bash
streamlit run dashboard.py
```

The dashboard will open in your web browser at `http://localhost:8501`

## Open311 (No Scraping) Parking Enforcement Heatmap

This project now supports an **API-only** pipeline using Austin’s documented Open311 API. It produces a **public-safe aggregated heatmap** of where people request parking violation enforcement.

Important: this is **not** a parking citation lookup or ticket map. It visualizes **311 requests** for enforcement.

### 1) Discover the `service_code`

The Open311 `services.json` endpoint can be unreliable at times, so the easiest discovery method is to search recent requests by text and see which `service_code` appears.

```bash
python open311_ingest.py discover "Parking Violation" --per-page 100
```

Pick the `service_code` whose `service_name` matches “Parking Violation Enforcement”.

### 2) Ingest Open311 requests (90-day backfill)

Open311 date filters are limited to 90-day windows, so ingestion automatically backfills in 90-day slices.

```bash
python open311_ingest.py ingest --service-code <SERVICE_CODE> --start-date 2025-01-01T00:00:00Z --per-page 100 --min-interval-s 6.2 --db-path 311_categories.db
```

### 3) Aggregate into spatial bins (public-safe)

This writes `open311_parking_heatmap_bins` into the same SQLite DB. By default it suppresses bins with fewer than 3 requests.

```bash
python open311_aggregate_heatmap.py run --service-code <SERVICE_CODE> --window-days 30 --bin-precision 3 --min-count 3 --db-path 311_categories.db
```

### 4) View the heatmap

```bash
streamlit run open311_parking_heatmap_app.py
```

### Notes

- **Rate limiting**: the API is rate limited (often 10 req/min). The ingest command now paces itself with `--min-interval-s` when no API key is provided.
- **Paging behavior**: ingestion uses `per_page` + `page` and keeps paging until empty (with safety checks for repeated pages/no-new IDs).
- **API key**: if you request an API key, pass it with `--api-key ...` for higher limits.
- **Privacy**: the app is designed to show **aggregates** by default (bins), not individual request addresses.

### Dashboard Features

- **Date Range Filter**: Select specific time periods to analyze
- **Department Filter**: Focus on specific city departments
- **Service Requests Over Time**: Line chart showing daily complaint volume
- **Top Service Descriptions**: Bar chart of most common complaint types
- **Group Descriptions**: Categorized view of infrastructure issues
- **Method Received**: How complaints are submitted to 311
- **Raw Data View**: Optional table view of filtered data

### Data Processing

The `filter_csv.py` script:
- Processes the raw 311 dataset (typically >100MB)
- Filters to include only requests from the last 365 days
- Handles date parsing for Austin's specific date format
- Outputs a manageable CSV file for the dashboard

### 311 Category Search Tool (NEW)

The `search_311_categories.py` tool provides flexible scraping of any Austin 311 service category:

#### List Available Categories
```bash
search-311 --list
```

#### Search Specific Categories
```bash
# Scrape bicycle complaints (original functionality)
python search_311_categories.py "Bicycle Issues"

# Scrape parking violations
python search_311_categories.py "Parking Violation Enforcement"

# Scrape multiple categories
python search_311_categories.py "Pothole Repair" "Traffic Signal - Maintenance"

# Limit pages and control scraping rate
python search_311_categories.py "Bicycle Issues" --max-pages 10 --chunk-size 20 --sleep 15
```

#### Features
- **127+ Categories**: Automatically extracts all available service categories from Austin 311
- **Flexible Search**: Search by partial category names (e.g., "parking" finds "Parking Violation Enforcement")
- **Smart Continuation**: Resumes scraping from where it left off
- **Rate Limiting**: Respectful scraping with configurable delays
- **Dual Storage**: Saves to both SQLite database and Parquet files
- **Category Tracking**: Each request tagged with category name and code

#### Database Structure
- Single `service_requests` table with category columns
- Indexed by `category_code` and `created_date` for fast queries
- Supports cross-category analysis and comparisons

## Data Source

This project uses the Austin 311 Service Requests dataset from Austin Transportation and Public Works. The data includes:
- Service request descriptions and categories
- Creation dates and timestamps
- Department assignments
- Reporting methods (phone, web, mobile app)
- Geographic information (when available)

## Technologies Used

- **Python**: Core data processing language
- **Pandas**: Data manipulation and analysis
- **Streamlit**: Interactive web dashboard framework
- **Plotly**: Interactive data visualizations
- **PowerShell**: Alternative data processing script

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-analysis`)
3. Commit your changes (`git commit -am 'Add new analysis feature'`)
4. Push to the branch (`git push origin feature/new-analysis`)
5. Create a Pull Request

## Future Enhancements

- ✅ **Web Scraping Integration**: COMPLETED - Automated scraping of any Austin 311 category
  - General tool supports 127+ service categories
  - Extracts: Address, Description, TicketNumber, Response, Status
  - Stores in SQLite + Parquet formats with category tracking
  - Smart continuation and rate limiting
- **Automated Complaint Categorization**: Group scraped data by most common complaint types
- **Geographic mapping of complaint hotspots**
- **Machine learning for automatic categorization**
- **Comparative analysis with other cities**
- **Mobile-responsive dashboard design**
- **Automated reporting and alerts**

## License

This project is open source and available under the MIT License.

## Acknowledgments

- City of Austin for providing open 311 data
- Austin Transportation and Public Works department
- The Austin cycling community for inspiring this analysis
