import os
import time
import logging
import queue
import threading
from fetcher import Fetcher
from scraper import Scraper
from downloader import Downloader
from data_saver import DataSaver
from config import NUM_JOBS, HTML_FILE_LIMIT, FETCHER_THREADS, SCRAPER_THREADS, DATA_FOLDER
from progress_tracker import ProgressTracker

def setup_logging():
    """
    Initializes logging to write detailed logs to 'scraper.log' inside 'data/'.
    """
    log_path = os.path.join(DATA_FOLDER, "scraper.log")
    os.makedirs(DATA_FOLDER, exist_ok=True)

    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.CRITICAL)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("Logging initialized successfully.")

    # Ensure log file is created and flushed before ProgressTracker starts
    file_handler.flush()
    os.fsync(file_handler.stream.fileno())

    # Small delay before ProgressTracker starts reading logs
    time.sleep(1)

def main():
    """
    Entry point for the job scraper. Manages the end-to-end workflow.
    """
    try:
        setup_logging()
        logging.info("Starting job scraper...")

        # Initialize job queue
        job_queue = queue.Queue()
        download_queue = queue.Queue()  # Queue for URLs to download

        # Initialize ProgressTracker (Fixes unresolved reference)
        progress_tracker = ProgressTracker(NUM_JOBS, HTML_FILE_LIMIT)

        ### Step 1: Fetch Jobs (Wait for Completion)
        fetcher = Fetcher(job_queue, progress_tracker, download_queue)  # Fetcher is initialized first

        fetcher_threads = []
        for _ in range(FETCHER_THREADS):
            thread = threading.Thread(target=fetcher.fetch_jobs, daemon=True)
            thread.start()
            fetcher_threads.append(thread)

        # Wait for all fetcher threads to complete
        for thread in fetcher_threads:
            thread.join()
        logging.info("Fetching completed.")

        ### Only initialize scraper & downloader AFTER fetching is done!
        data_saver = DataSaver()
        scraper = Scraper(job_queue, progress_tracker, data_saver)  # Scraper initialized after fetching
        downloader = Downloader(progress_tracker)  # Downloader initialized after fetching

        ### Step 2: Scrape Jobs (Wait for Completion)
        scraper_threads = []
        batch_size = 50
        for _ in range(SCRAPER_THREADS):
            thread = threading.Thread(target=scraper.scrape_jobs, daemon=True)
            thread.start()
            scraper_threads.append(thread)

        # Periodically save jobs while scraping
        while any(thread.is_alive() for thread in scraper_threads):
            if len(scraper.scraped_jobs) >= batch_size:
                data_saver.batch_save_jobs(scraper.scraped_jobs)
                scraper.scraped_jobs.clear()

        # Wait for all scrapers to complete
        for thread in scraper_threads:
            thread.join()
        logging.info("Scraping completed.")

        # Ensure final batch save for any remaining jobs
        if scraper.scraped_jobs:
            data_saver.batch_save_jobs(scraper.scraped_jobs)

        ### Step 3: Download HTML Files (Wait for Completion)
        download_threads = []
        for _ in range(3):  # Use 3 threads for faster downloads
            thread = threading.Thread(target=downloader.download_jobs, args=(download_queue,))
            thread.start()
            download_threads.append(thread)

        # Wait for all download threads to finish
        for thread in download_threads:
            thread.join()
        progress_tracker.complete()
        logging.info("Downloading completed.")

        ### Step 4: Zip HTML Files (Once All Jobs are Processed)
        downloader.zip_html_files()
        logging.info("Zipping completed.")

        ### Final Step: Clean Exit
        logging.info("Fetching, Scraping, Downloading & Zipping Completed. Exiting program.")
        print("\nFetching, Scraping, Downloading & Zipping Completed. Exiting program.")

    except Exception as e:
        logging.error(f"Unexpected error in main execution: {e}")

if __name__ == "__main__":
    main()