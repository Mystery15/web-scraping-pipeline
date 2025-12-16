# web-scraping-pipeline
A web scraping pipeline is an automated system that extracts raw data from websites, processes and cleans it (transformation), and then stores or loads it into a database or file for analysis, turning unstructured web content into structured, usable information for tasks like market research, price monitoring, or AI training. It's a multi-stage workflow involving crawling, extraction, cleaning, transformation, and loading (ETL), often orchestrated by tools like Apache Airflow and built with Python libraries (BeautifulSoup, Scrapy). 

A comprehensive web scraping pipeline for collecting structured data from websites, storing it in SQLite/CSV, and scheduling automated runs.

## Features

- **Multi-target scraping**: Scrape books and products from different websites
- **Data persistence**: Store data in SQLite database and export to CSV
- **Scheduling**: Automated scheduling with configurable intervals
- **Error handling**: Robust error handling with retry logic
- **Logging**: Comprehensive logging for monitoring and debugging
- **Statistics**: Track scraping performance and success rates


## System Architecture

┌─────────────────┐
│   Scheduler     │
│   (Schedule)    │
└────────┬────────┘
         │
┌────────▼────────┐
│   Main Scraper  │
│   (scraper.py)  │
└────────┬────────┘
         │
┌────────▼────────┐    ┌─────────────────┐
│   Data Parser   │───▶│   SQLite DB     │
│   (BeautifulSoup)│   │   (data.db)     │
└────────┬────────┘    └─────────────────┘
         │
┌────────▼────────┐    ┌─────────────────┐
│   Data Cleaner  │───▶│   CSV Files     │
│   (pandas)      │    │   (output/)     │
└─────────────────┘    └─────────────────┘

## Installation

1. Clone or download this repository
2. Install dependencies:
```bash
pip install -r requirements.txt