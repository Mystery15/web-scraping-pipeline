"""
Configuration settings for the web scraping pipeline
"""

# Scraping configuration
SCRAPING_CONFIG = {
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'timeout': 10,
    'max_retries': 3,
    'delay_between_requests': 1,  # seconds
}

# Target URLs to scrape
TARGET_URLS = {
    'books': [
        'https://pdfdrive.com.co/',
        'https://pdfdrive.com.co/',
    ],
    'products': [
        'https://webscraper.io/test-sites/e-commerce/allinone',
    ]
}

# Database configuration
DATABASE_CONFIG = {
    'database_url': 'sqlite:///data.db',
    'echo_sql': False,
}

# Output configuration
OUTPUT_CONFIG = {
    'csv_output_dir': 'output',
    'json_output_dir': 'output/json',
}

# Scheduling configuration
SCHEDULING_CONFIG = {
    'daily_at': '02:00',  # Run daily at 2 AM
    'interval_hours': 24,
}