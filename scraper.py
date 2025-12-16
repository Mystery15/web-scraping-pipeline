"""
Main web scraper using BeautifulSoup
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
import os

from config import SCRAPING_CONFIG, TARGET_URLS
from database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': SCRAPING_CONFIG['user_agent']
        })
        self.db = DatabaseManager()
        self._setup_output_dirs()
    
    def _setup_output_dirs(self):
        """Create output directories if they don't exist"""
        os.makedirs('output', exist_ok=True)
        os.makedirs('output/json', exist_ok=True)
    
    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content with retry logic"""
        for attempt in range(SCRAPING_CONFIG['max_retries']):
            try:
                response = self.session.get(url, timeout=SCRAPING_CONFIG['timeout'])
                response.raise_for_status()
                logger.info(f"Successfully fetched {url}")
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < SCRAPING_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {SCRAPING_CONFIG['max_retries']} attempts")
                    return None
    
    def scrape_books(self, urls: List[str]) -> List[Dict]:
        """Scrape books from bookstoscrape.com"""
        all_books = []
        
        for url in urls:
            logger.info(f"Scraping books from: {url}")
            html_content = self.fetch_page(url)
            
            if not html_content:
                continue
            
            soup = BeautifulSoup(html_content, 'html.parser')
            books = soup.find_all('article', class_='product_pod')
            
            for book in books:
                try:
                    book_data = {
                        'title': book.h3.a['title'],
                        'price': float(book.find('p', class_='price_color').text.replace('£', '')),
                        'rating': book.p['class'][1],
                        'availability': book.find('p', class_='instock availability').text.strip(),
                        'category': url.split('/')[-2] if len(url.split('/')) >= 2 else 'unknown',
                        'url': f"http://books.toscrape.com/catalogue/{book.h3.a['href']}",
                        'description': self._get_book_description(book.h3.a['href'])
                    }
                    all_books.append(book_data)
                    logger.debug(f"Scraped book: {book_data['title']}")
                except Exception as e:
                    logger.error(f"Error parsing book: {e}")
            
            time.sleep(SCRAPING_CONFIG['delay_between_requests'])
        
        return all_books
    
    def _get_book_description(self, book_path: str) -> str:
        """Get detailed description from individual book page"""
        try:
            url = f"http://books.toscrape.com/catalogue/{book_path.replace('../../../', '')}"
            html = self.fetch_page(url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                description = soup.find('meta', attrs={'name': 'description'})
                return description['content'] if description else ''
        except:
            return ''
        return ''
    
    def scrape_products(self, urls: List[str]) -> List[Dict]:
        """Scrape products from webscraper.io test site"""
        all_products = []
        
        for url in urls:
            logger.info(f"Scraping products from: {url}")
            html_content = self.fetch_page(url)
            
            if not html_content:
                continue
            
            soup = BeautifulSoup(html_content, 'html.parser')
            products = soup.find_all('div', class_='thumbnail')
            
            for product in products:
                try:
                    # Extract product name
                    name_elem = product.find('a', class_='title')
                    name = name_elem.text.strip() if name_elem else 'N/A'
                    
                    # Extract price
                    price_elem = product.find('h4', class_='price')
                    price = float(price_elem.text.replace('$', '')) if price_elem else 0.0
                    
                    # Extract description
                    desc_elem = product.find('p', class_='description')
                    description = desc_elem.text.strip() if desc_elem else ''
                    
                    # Extract rating
                    rating_elem = product.find('p', {'data-rating': True})
                    rating = float(rating_elem['data-rating']) if rating_elem else 0.0
                    
                    # Extract review count
                    reviews_elem = product.find('p', class_='review-count')
                    reviews = int(reviews_elem.text.split()[0]) if reviews_elem else 0
                    
                    product_data = {
                        'name': name,
                        'price': price,
                        'description': description,
                        'rating': rating,
                        'reviews': reviews,
                        'category': self._extract_category(url),
                        'url': f"https://webscraper.io{name_elem['href']}" if name_elem else url
                    }
                    all_products.append(product_data)
                    logger.debug(f"Scraped product: {product_data['name']}")
                except Exception as e:
                    logger.error(f"Error parsing product: {e}")
            
            time.sleep(SCRAPING_CONFIG['delay_between_requests'])
        
        return all_products
    
    def _extract_category(self, url: str) -> str:
        """Extract category from URL"""
        try:
            return url.split('/')[-1] if '/' in url else 'all'
        except:
            return 'unknown'
    
    def save_to_csv(self, data: List[Dict], filename: str):
        """Save data to CSV file"""
        if not data:
            logger.warning(f"No data to save to {filename}")
            return
        
        try:
            df = pd.DataFrame(data)
            df.to_csv(f'output/{filename}', index=False, encoding='utf-8')
            logger.info(f"Saved {len(data)} records to output/{filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
    
    def run_scraping_job(self, target: str):
        """Run a complete scraping job for specific target"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting scraping job for: {target}")
            
            if target == 'books':
                books_data = self.scrape_books(TARGET_URLS['books'])
                records_saved = self.db.save_books(books_data)
                self.save_to_csv(books_data, 'books.csv')
                self.db.export_to_csv('books', 'output/books_latest.csv')
                
            elif target == 'products':
                products_data = self.scrape_products(TARGET_URLS['products'])
                records_saved = self.db.save_products(products_data)
                self.save_to_csv(products_data, 'products.csv')
                self.db.export_to_csv('products', 'output/products_latest.csv')
                
            else:
                raise ValueError(f"Unknown target: {target}")
            
            self.db.log_scraping_run(
                target=target,
                status='success',
                records_scraped=records_saved,
                start_time=start_time
            )
            
            logger.info(f"Completed scraping job for {target}. Saved {records_saved} records.")
            return True
            
        except Exception as e:
            logger.error(f"Scraping job failed for {target}: {e}")
            self.db.log_scraping_run(
                target=target,
                status='failed',
                records_scraped=0,
                error_message=str(e),
                start_time=start_time
            )
            return False
    
    def run_all_scraping_jobs(self):
        """Run all scraping jobs"""
        logger.info("Starting all scraping jobs")
        
        results = {}
        for target in ['books', 'products']:
            results[target] = self.run_scraping_job(target)
            time.sleep(2)  # Brief pause between jobs
        
        # Generate report
        stats = self.db.get_scraping_stats()
        self._generate_report(stats, results)
        
        logger.info("All scraping jobs completed")
        return results
    
    def _generate_report(self, stats: Dict, results: Dict):
        """Generate scraping report"""
        report = f"""
        ========================================
        WEB SCRAPING PIPELINE - EXECUTION REPORT
        ========================================
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        SCRAPING RESULTS:
        - Books: {'SUCCESS' if results.get('books') else 'FAILED'}
        - Products: {'SUCCESS' if results.get('products') else 'FAILED'}
        
        DATABASE STATISTICS:
        - Total Books: {stats.get('total_books', 0)}
        - Total Products: {stats.get('total_products', 0)}
        - Last Book Scrape: {stats.get('last_book_scrape', 'Never')}
        - Last Product Scrape: {stats.get('last_product_scrape', 'Never')}
        - Overall Success Rate: {stats.get('success_rate', 0)}%
        
        OUTPUT FILES:
        - output/books.csv (latest scrape)
        - output/products.csv (latest scrape)
        - output/books_latest.csv (from database)
        - output/products_latest.csv (from database)
        - data.db (SQLite database)
        ========================================
        """
        
        print(report)
        
        # Save report to file
        with open('output/scraping_report.txt', 'w') as f:
            f.write(report)

def main():
    """Main execution function"""
    scraper = WebScraper()
    
    print("""
    Web Scraping Pipeline
    ======================
    1. Scrape Books
    2. Scrape Products
    3. Run All Jobs
    4. Export Database to CSV
    5. View Statistics
    6. Exit
    """)
    
    while True:
        choice = input("\nSelect an option (1-6): ").strip()
        
        if choice == '1':
            scraper.run_scraping_job('books')
        elif choice == '2':
            scraper.run_scraping_job('products')
        elif choice == '3':
            scraper.run_all_scraping_jobs()
        elif choice == '4':
            scraper.db.export_to_csv('books')
            scraper.db.export_to_csv('products')
        elif choice == '5':
            stats = scraper.db.get_scraping_stats()
            print("\nScraping Statistics:")
            for key, value in stats.items():
                print(f"  {key.replace('_', ' ').title()}: {value}")
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()