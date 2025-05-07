import re
import time
import random
import logging
import requests
import threading
import queue
import urllib.parse
from queue import Queue
from bs4 import BeautifulSoup
from config import USER_AGENTS, MAX_RETRIES, API_KEY, NUM_JOBS, FETCHER_THREADS, HTML_FILE_LIMIT, JOB_TITLES


class Fetcher:
    """
    Fetches job listing pages and extracts job links from jobs.ch.
    Uses a queue-based system to enable parallel fetching.
    """

    def __init__(self, job_queue=None, progress_tracker=None, download_queue=None):
        """
        Initialize Fetcher with a queue to store job links and a progress tracker.
        """
        self.job_queue = job_queue
        self.progress = progress_tracker  # Track fetching progress
        self.download_queue = download_queue if download_queue else Queue()  # Ensure it exists
        self.seen_urls = set()  # Track already fetched job links
        self.seen_urls_lock = threading.Lock()  # Ensure thread safety
        self.fetching_complete = threading.Event()  # Flag to indicate fetching completion
        self.page_queue = Queue()  # Pages to process
        self.fetched_count = 0  # Tracks how many jobs have been fetched in total
        self.count_lock = threading.Lock()  # Ensures thread-safe updates

        # Page Number Estimation
        self.NUM_PAGES = (NUM_JOBS // 2000 + 30)  # 20 jobs per page (approx.), add buffer

        # Populate page queue
        for i in range(1, self.NUM_PAGES + 1):
            self.page_queue.put(i)

    @staticmethod
    def fetch_page(url):
        """
        Fetch a page using the ScrapingBee API.
        Implements retries on failures.
        """
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        params = {"api_key": API_KEY, "url": url, "render_js": "true"}

        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get("https://app.scrapingbee.com/api/v1/", params=params, headers=headers,
                                        timeout=15)
                response.raise_for_status()
                return response.text
            #except requests.exceptions.Timeout:
                #logging.error(f"Timeout error while fetching {url}")
            except requests.exceptions.TooManyRedirects:
                logging.error(f"Too many redirects for {url}")
                return None
            except requests.exceptions.RequestException as err:
                logging.error(f"Request failed: {err} for {url}")
                return None

            retries += 1
            time.sleep(2 ** retries)  # Exponential backoff

        return None

    @staticmethod
    def extract_job_links(html_content):
        """
        Extracts job links from a jobs.ch listing page.
        :param html_content: HTML of the job listing page
        :return: A list of job URLs
        """
        soup = BeautifulSoup(html_content, "html.parser")
        job_links = soup.select("a[href^='/en/vacancies/detail/']")
        return ["https://www.jobs.ch" + link["href"] for link in job_links]

    def fetch_jobs(self):
        """
        Start multiple threads for fetching jobs.
        """
        threads = []

        for _ in range(FETCHER_THREADS):
            thread = threading.Thread(target=self._fetch_pages)
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check if fetched count is still less than NUM_JOBS
        while self.fetched_count < NUM_JOBS:
            #logging.warning(f"Only {self.fetched_count}/{NUM_JOBS} jobs fetched. Retrying...")
            self._fetch_pages()  # Fetch more pages if needed

        logging.info("Fetching completed.")
        self.fetching_complete.set()  # Signal scrapers that fetching is done

    def get_total_pages(self, encoded_job_title):
        """
        Fetch the total number of pages available for a job title.
        Ensures the correct number is returned to `_fetch_pages()`.
        """
        url = f"https://www.jobs.ch/en/vacancies/?page=1&term={encoded_job_title}"
        html = self.fetch_page(url)

        if not html:
            logging.error(f"Failed to fetch first page for '{encoded_job_title}'")
            return 0

        try:
            # Extract numPages specifically from the "meta" section
            matches = re.findall(r'"meta":\s*{\s*"numPages":\s*(\d+)', html)

            if matches:
                total_pages = int(matches[0])  # First match should be the correct one
                #logging.info(f"Found {total_pages} pages for '{encoded_job_title}'")
                return total_pages

                # If regex fails, try parsing with BeautifulSoup as a fallback
            soup = BeautifulSoup(html, "html.parser")
            script_tags = soup.find_all("script")

            for script in script_tags:
                if script.string and '"meta":' in script.string:
                    match = re.search(r'"meta":\s*{\s*"numPages":\s*(\d+)', script.string)
                    if match:
                        total_pages = int(match.group(1))
                        logging.info(f"Extracted {total_pages} pages for '{encoded_job_title}' from script tag")
                        return total_pages

                        # If still not found, log an error and return a safe fallback
            #logging.warning(f"Could not find total page count for '{encoded_job_title}' in HTML.")
            #logging.debug(f"HTML snippet for '{encoded_job_title}': {html[:1000]}")
            return 1  # Assume at least 1 page exists

        except Exception as e:
            logging.error(f"Error extracting pages for '{encoded_job_title}': {e}")
            return 0

    def _fetch_pages(self):
        """
        Fetch job links for each job title, going through all available pages.
        Stops when NUM_JOBS is reached.
        """
        for job_title in JOB_TITLES:  # Loop through all job titles sequentially
            encoded_job_title = urllib.parse.quote_plus(job_title)  # Encode for URL

            # Get the actual number of pages available for this job title
            total_pages = self.get_total_pages(encoded_job_title)

            if total_pages == 0:
                logging.warning(f"No jobs found for '{job_title}', skipping...")
                continue  # Skip this job title if there are no jobs

            logging.info(f"Fetching {total_pages} pages for '{job_title}'")

            for current_page in range(1, total_pages + 1):
                with self.count_lock:
                    if self.fetched_count >= NUM_JOBS:
                        return  # Stop fetching if we reached the limit

                url = f"https://www.jobs.ch/en/vacancies/?page={current_page}&term={encoded_job_title}"
                #logging.debug(f"Fetching page {current_page} for '{job_title}': {url}")

                html_content = self.fetch_page(url)

                if html_content:
                    job_links = self.extract_job_links(html_content)
                    logging.debug(f"Extracted {len(job_links)} job links from page {current_page} for '{job_title}'")

                    if not job_links:
                        logging.warning(
                            f"No jobs found on page {current_page} for '{job_title}'. Moving to next keyword.")
                        break  # Stop searching this job title and go to the next

                    with self.seen_urls_lock:
                        new_links = [link for link in job_links if link not in self.seen_urls]

                    if new_links:
                        for link in new_links:
                            with self.count_lock:
                                if self.fetched_count >= NUM_JOBS:
                                    return  # Stop adding jobs if we hit the limit

                            self.job_queue.put(link)  # Add to job queue
                            if self.fetched_count < HTML_FILE_LIMIT:
                                self.download_queue.put(link)  # Add to download queue
                            self.fetched_count += 1
                            self.progress.update_fetch()

                        with self.seen_urls_lock:
                            self.seen_urls.update(new_links)  # Prevent duplicate fetching

                else:
                    logging.error(f"Failed to fetch job listing page {current_page} for '{job_title}'")
                    break  # Stop searching this job title if the page fails

                time.sleep(random.uniform(0.5, 1.5))  # Rate limiting delay