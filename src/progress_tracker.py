import sys
import threading
import logging
import time
import re

class ProgressTracker:
    """
    Tracks and displays the progress of fetching, scraping, and downloading jobs.
    Ensures accurate, real-time updates while preventing over-counting.
    """

    def __init__(self, total_jobs, html_file_limit, log_file_path="scraper.log"):
        """
        Initializes the progress tracker.

        :param total_jobs: Total number of jobs to be processed.
        :param html_file_limit: Maximum number of HTML files to be downloaded.
        :param log_file_path: Path to the log file for tracking scraped jobs.
        """
        self.total_jobs = total_jobs
        self.html_file_limit = html_file_limit
        self.log_file_path = log_file_path  # Store log file path

        # Counters for job progress
        self.fetched = 0
        self.scraped = 0
        self.downloaded = 0

        # State flags to control progress transitions
        self.fetching_done = False
        self.scraping_done = False
        self.downloading_done = False

        # Lock for thread-safe updates
        self.lock = threading.Lock()

        # Start the live counter when the program starts
        self.running = True  # Control flag for counter
        self.start_time = time.time()  # Store the start time

        # Start progress display thread
        self.progress_thread = threading.Thread(target=self._live_counter, daemon=True)
        self.progress_thread.start()

        # Start log parser thread for scraping progress
        self.log_parser_thread = threading.Thread(target=self._parse_log_for_scraped_jobs, daemon=True)
        self.log_parser_thread.start()

        sys.stdout.flush()

    def _live_counter(self):
        """
        Runs in a background thread and continuously updates the terminal with a time-based counter.
        """
        while self.running:
            elapsed_time = time.time() - self.start_time  # Calculate elapsed time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)

            with self.lock:
                sys.stdout.write(f"\r[Time: {minutes:02d}:{seconds:02d}] "
                                 f"Fetched: {self.fetched}/{self.total_jobs} | "
                                 f"Scraped: {self.scraped}/{self.total_jobs} | "
                                 f"Downloaded: {self.downloaded}/{self.html_file_limit}")
                sys.stdout.flush()

            time.sleep(0.1)  # Updates every 100ms for smooth tracking

    def _parse_log_for_scraped_jobs(self):
        """
        Monitors the log file in real-time to count scraped job entries.
        """
        scraped_pattern = re.compile(r"Scraped job data: {'id': (\d+), 'url'")

        try:
            with open(self.log_file_path, "r") as log_file:
                log_file.seek(0, 2)  # Move to the end of the file

                while self.running:
                    line = log_file.readline()
                    if not line:
                        time.sleep(0.5)  # Wait and retry if no new line appears
                        continue

                    if scraped_pattern.search(line):
                        self.update_scrape()

        except FileNotFoundError:
            logging.error(f"Log file {self.log_file_path} not found.")
            return

    def update_fetch(self):
        """
        Updates the counter for fetched jobs and refreshes terminal output.
        Ensures fetch count does not exceed total_jobs.
        """
        with self.lock:
            if self.fetched < self.total_jobs:
                self.fetched += 1

            # Ensure fetching completion prints ONCE
            if self.fetched == self.total_jobs and not self.fetching_done:
                self.fetching_done = True

    def update_scrape(self):
        """
        Updates the counter for scraped jobs and refreshes terminal output.
        Ensures scraped count never exceeds fetched count.
        """
        with self.lock:
            if not self.fetching_done:
                return  # Ensure fetching is completed before scraping starts

            if self.scraped < self.fetched:  # Prevents over-counting scraping
                self.scraped += 1

            if self.scraped == self.total_jobs and not self.scraping_done:
                self.scraping_done = True
                sys.stdout.flush()

    def update_download(self):
        """
        Updates the counter for downloaded HTML files and refreshes terminal output.
        Ensures downloaded count does not exceed HTML_FILE_LIMIT.
        """
        with self.lock:
            if self.downloaded < self.html_file_limit:
                self.downloaded += 1
                #logging.info(f"Updated download count: {self.downloaded}/{self.html_file_limit}")

    def stop(self):
        """
        Stops the live counter when all tasks are done.
        """
        self.running = False
        self.progress_thread.join()

    def complete(self):
        """
        Displays the final completion message when all steps are done.
        """
        max_retries = 10  # Prevent infinite loop
        retries = 0

        while not self.downloading_done and retries < max_retries:
            logging.info(f"Still waiting: fetch={self.fetching_done}, "
                         f"scrape={self.scraping_done}, download={self.downloading_done}")
            time.sleep(1)

        with self.lock:
            sys.stdout.write(f"\r[Time: {int(time.time() - self.start_time):02d}] "
                             f"Fetched: {self.fetched}/{self.total_jobs} | "
                             f"Scraped: {self.scraped}/{self.total_jobs} | "
                             f"Downloaded: {self.downloaded}/{self.html_file_limit}\n")
            sys.stdout.flush()

        #print("\nFetching, Scraping & Downloading Completed!")
        logging.info("Fetching, Scraping & Downloading Completed.")
