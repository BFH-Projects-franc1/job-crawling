import logging
import threading
import queue
from bs4 import BeautifulSoup
from fetcher import Fetcher
from config import SCRAPER_THREADS


class Scraper:
    """
    Scrapes job details from fetched job pages using multiple threads.
    Uses a queue-based system for efficient processing.
    """

    def __init__(self, job_queue, progress_tracker, data_saver):
        """
        Initialize Scraper with a job queue, progress tracker, and shared data saver.
        :param job_queue: Queue containing job URLs to scrape.
        :param progress_tracker: Instance of ProgressTracker for tracking scraping progress.
        :param data_saver: Shared DataSaver instance to avoid duplicates.
        """
        self.job_queue = job_queue
        self.progress = progress_tracker
        self.data_saver = data_saver  # Use shared instance!
        self.scraped_jobs = []
        self.job_id = 1

        self.seen_jobs = set()  # Track already scraped jobs
        self.lock = threading.Lock()  # Ensure thread safety

    def scrape_job(self, job_url):
        """
        Scrape details from a job listing page.
        :param job_url: URL of the job page.
        """
        with self.lock:
            if job_url in self.seen_jobs:
                logging.warning(f"Skipping duplicate job: {job_url}")
                return None  # Skip duplicate scraping
            self.seen_jobs.add(job_url)  # Mark as scraped **inside lock**

        job_html = Fetcher().fetch_page(job_url)
        if not job_html:
            logging.error(f"Failed to fetch job page: {job_url}")
            return None  # Skip failed jobs

        soup = BeautifulSoup(job_html, "html.parser")

        with self.lock:
            job_details = {
                "id": self.job_id,
                "url": job_url,
                "title": self.get_text(soup, "title"),
                "publication_date": self.get_text(soup, "Publication date:") or "N/A",
                "workload": self.get_text(soup, "Workload:") or "N/A",
                "contract_type": self.get_text(soup, "Contract type:") or "N/A",
                "salary": self.get_text(soup, "Salary:") or "N/A",
                "languages": self.get_text(soup, "Language:") or "N/A",
                "place_of_work": self.get_text(soup, "Place of work:") or "N/A"
            }
            self.job_id += 1  # Increment ID

        logging.info(f"Scraped job data: {job_details}")

        self.progress.update_scrape()  # Update progress
        self.scraped_jobs.append(job_details)


        return job_details

    @staticmethod
    def get_text(soup, label):
        """
        Extracts job details based on label text.
        :param soup: Parsed HTML of the job page.
        :param label: Label to search for (e.g., 'Workload:').
        :return: Extracted text or 'N/A' if not found.
        """
        if label == "title":
            return Scraper.extract_title(soup)  # Use title extraction method

        tag = soup.find("span", string=lambda text: text and text.strip() == label)
        return tag.find_next("span").text.strip() if tag and tag.find_next("span") else "N/A"

    @staticmethod
    def extract_title(soup):
        """
        Extracts the job title from the job page.
        :param soup: Parsed HTML of the job page.
        :return: Job title or 'N/A' if not found.
        """
        job_title = "N/A"

        # Try extracting from <title> tag
        job_title_tag = soup.find("title")
        if job_title_tag:
            job_title = job_title_tag.text.split(" - Job Offer")[0].strip()

        # If <title> fails, try <h1> (common for job titles)
        if job_title == "N/A":
            h1_tag = soup.find("h1")
            if h1_tag:
                job_title = h1_tag.text.strip()

        # If still no title, try a class-based lookup
        if job_title == "N/A":
            title_div = soup.find("div", class_="job-title")
            if title_div:
                job_title = title_div.text.strip()

        return job_title

    def worker(self):
        """
        Worker function for each scraping thread.
        Pulls job URLs from the queue, scrapes them, and saves the results.
        """
        while True:
            try:
                job_url = self.job_queue.get(timeout=10)
                job_details = self.scrape_job(job_url)
                if job_details:
                    with self.lock:  # Ensure thread safety when modifying the shared list
                        pass

                self.job_queue.task_done()  # Mark task as done
            except queue.Empty:
                logging.info("Scraper worker found empty queue, stopping.")
                break  # Stop if queue is empty

    def scrape_jobs(self):
        """
        Start multiple threads to scrape jobs from the queue in parallel.
        """
        threads = []

        # Create scraper threads
        for _ in range(SCRAPER_THREADS):
            thread = threading.Thread(target=self.worker)
            thread.start()
            threads.append(thread)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        logging.info("Scraping process finished.")
        return self.scraped_jobs
