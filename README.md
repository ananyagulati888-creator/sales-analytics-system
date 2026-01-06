# Sales Analytics System

A complete Python-based sales analytics application that processes raw sales data, performs detailed analysis, integrates external product data using an API, enriches transactions, and generates a comprehensive business report.

This project demonstrates skills in **file handling, data cleaning, validation, analytics, API integration, and report generation**.

---

## Features

- Handles non-UTF encoded sales data files
- Cleans and validates transaction records
- Supports optional filtering by region and transaction amount
- Performs advanced sales analytics:
  - Total revenue calculation
  - Region-wise performance analysis
  - Top-selling and low-performing products
  - Customer purchase behavior analysis
  - Date-based sales trends and peak sales day
- Integrates external product data using the DummyJSON API
- Enriches sales data with product category, brand, and ratings
- Generates:
  - Enriched sales data file
  - Detailed text-based sales report

---

## Project Structure

sales-analytics-system/
├── data/
│ ├── sales_data.txt # Raw input data (provided)
│ └── enriched_sales_data.txt # Enriched output data
│
├── output/
│ └── sales_report.txt # Final analytics report
│
├── utils/
│ ├── file_handler.py # File reading & preprocessing
│ ├── data_processor.py # Data cleaning, validation & analytics
│ └── api_handler.py # API integration & enrichment
│
├── main.py # Main application workflow
├── requirements.txt # Project dependencies
└── README.md


---

## Requirements

- Python 3.9 or above
- Internet connection (for API integration)

### Python Libraries
- `requests`

Install dependencies using:
```bash
pip install -r requirements.txt
