"""
Database operations for storing scraped data
"""
import sqlite3
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, DateTime
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime
import logging
from config import DATABASE_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(DATABASE_CONFIG['database_url'], echo=DATABASE_CONFIG['echo_sql'])
        self.metadata = MetaData()
        self._define_tables()
        self._create_tables()
        
    def _define_tables(self):
        """Define database table schemas"""
        self.books = Table('books', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('title', String(500)),
            Column('price', Float),
            Column('rating', String(10)),
            Column('availability', String(50)),
            Column('category', String(100)),
            Column('url', String(500)),
            Column('scraped_at', DateTime, default=datetime.now),
            Column('description', String(2000))
        )
        
        self.products = Table('products', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(500)),
            Column('price', Float),
            Column('description', String(2000)),
            Column('rating', Float),
            Column('reviews', Integer),
            Column('category', String(100)),
            Column('url', String(500)),
            Column('scraped_at', DateTime, default=datetime.now)
        )
        
        self.scraping_logs = Table('scraping_logs', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('target', String(100)),
            Column('status', String(50)),
            Column('records_scraped', Integer),
            Column('error_message', String(1000)),
            Column('start_time', DateTime),
            Column('end_time', DateTime, default=datetime.now),
            Column('duration_seconds', Float)
        )
    
    def _create_tables(self):
        """Create tables if they don't exist"""
        try:
            self.metadata.create_all(self.engine)
            logger.info("Database tables created/verified successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {e}")
    
    def save_books(self, books_data):
        """Save books data to database"""
        return self._save_data(books_data, 'books')
    
    def save_products(self, products_data):
        """Save products data to database"""
        return self._save_data(products_data, 'products')
    
    def _save_data(self, data, table_name):
        """Generic method to save data to database"""
        if not data:
            return 0
        
        try:
            with self.engine.connect() as conn:
                if table_name == 'books':
                    conn.execute(self.books.insert(), data)
                elif table_name == 'products':
                    conn.execute(self.products.insert(), data)
                conn.commit()
            
            logger.info(f"Successfully saved {len(data)} records to {table_name} table")
            return len(data)
            
        except SQLAlchemyError as e:
            logger.error(f"Error saving to {table_name}: {e}")
            return 0
    
    def log_scraping_run(self, target, status, records_scraped=0, error_message=None, start_time=None):
        """Log scraping run details"""
        duration = None
        if start_time:
            duration = (datetime.now() - start_time).total_seconds()
        
        log_entry = {
            'target': target,
            'status': status,
            'records_scraped': records_scraped,
            'error_message': error_message,
            'start_time': start_time,
            'duration_seconds': duration
        }
        
        try:
            with self.engine.connect() as conn:
                conn.execute(self.scraping_logs.insert(), log_entry)
                conn.commit()
            logger.info(f"Logged scraping run for {target}: {status}")
        except SQLAlchemyError as e:
            logger.error(f"Error logging scraping run: {e}")
    
    def export_to_csv(self, table_name, filename=None):
        """Export table data to CSV"""
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, self.engine)
            
            if filename is None:
                filename = f"output/{table_name}.csv"
            
            df.to_csv(filename, index=False)
            logger.info(f"Exported {len(df)} records from {table_name} to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting {table_name} to CSV: {e}")
            return None
    
    def get_scraping_stats(self):
        """Get scraping statistics"""
        try:
            with sqlite3.connect('data.db') as conn:
                cursor = conn.cursor()
                
                # Get total records
                cursor.execute("SELECT COUNT(*) FROM books")
                total_books = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM products")
                total_products = cursor.fetchone()[0]
                
                # Get latest scrape
                cursor.execute("SELECT MAX(scraped_at) FROM books")
                last_book_scrape = cursor.fetchone()[0]
                
                cursor.execute("SELECT MAX(scraped_at) FROM products")
                last_product_scrape = cursor.fetchone()[0]
                
                # Get success rate
                cursor.execute("""
                    SELECT 
                        COUNT(CASE WHEN status = 'success' THEN 1 END) as success,
                        COUNT(*) as total
                    FROM scraping_logs
                """)
                success, total = cursor.fetchone()
                success_rate = (success / total * 100) if total > 0 else 0
                
                return {
                    'total_books': total_books,
                    'total_products': total_products,
                    'last_book_scrape': last_book_scrape,
                    'last_product_scrape': last_product_scrape,
                    'success_rate': round(success_rate, 2)
                }
                
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}