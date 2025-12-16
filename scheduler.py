"""
Scheduling script for automated scraping runs
"""
import schedule
import time
import logging
from datetime import datetime
from scraper import WebScraper
from config import SCHEDULING_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/scraping_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_scheduled_scraping():
    """Execute scheduled scraping jobs"""
    logger.info("=" * 50)
    logger.info(f"Starting scheduled scraping run at {datetime.now()}")
    
    try:
        scraper = WebScraper()
        results = scraper.run_all_scraping_jobs()
        
        success_count = sum(1 for result in results.values() if result)
        logger.info(f"Scheduled run completed. Successful jobs: {success_count}/{len(results)}")
        
    except Exception as e:
        logger.error(f"Error during scheduled scraping: {e}")
    
    logger.info(f"Completed scheduled scraping run at {datetime.now()}")
    logger.info("=" * 50)

def setup_scheduler():
    """Setup and run the scheduler"""
    logger.info("Setting up scraping scheduler...")
    
    # Schedule daily run
    schedule.every().day.at(SCHEDULING_CONFIG['daily_at']).do(run_scheduled_scraping)
    
    # Alternatively, schedule at interval
    # schedule.every(SCHEDULING_CONFIG['interval_hours']).hours.do(run_scheduled_scraping)
    
    logger.info(f"Scheduler configured to run daily at {SCHEDULING_CONFIG['daily_at']}")
    logger.info("Press Ctrl+C to stop the scheduler")
    
    # Run immediately on startup (optional)
    logger.info("Running initial scrape...")
    run_scheduled_scraping()
    
    # Keep the scheduler running
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

def run_once():
    """Run scraping once (for manual execution)"""
    logger.info("Running one-time scraping job...")
    run_scheduled_scraping()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Web Scraping Scheduler')
    parser.add_argument('--once', action='store_true', help='Run scraping once and exit')
    parser.add_argument('--schedule', action='store_true', help='Run with scheduler (default)')
    
    args = parser.parse_args()
    
    if args.once:
        run_once()
    else:
        setup_scheduler()