import os
import logging
import zipfile
import queue
import threading
from config import DATA_FOLDER, HTML_FILE_LIMIT
from fetcher import Fetcher

class Downloader:
    """
    Manages downloading and storing job HTML pages.
    """

    def __init__(self, progress_tracker):
        """
        Initialize the downloader with a progress tracker.

        :param progress_tracker: Instance of ProgressTracker for tracking downloads.
        """
        self.num_html_saved = 0  # Tracks how many HTML files have been saved
        self.local_html_folder = os.path.join(DATA_FOLDER, "html")
        self.fetcher = Fetcher()  # Use Fetcher for requesting job pages
        self.progress = progress_tracker  # Track downloading progress independently
        self.lock = threading.Lock()

        # Ensure storage directory exists
        if not os.path.exists(self.local_html_folder):
            os.makedirs(self.local_html_folder)

    def download_jobs(self, download_queue):
        """
        Continuously fetches job URLs from queue and saves them as HTML.

        :param download_queue: Queue containing job URLs to download.
        """
        if download_queue.empty():
            logging.warning("No jobs available for download.")
            return

        logging.info("Starting downloading process...")

        while True:
            try:
                job_url = download_queue.get(timeout=10)  # Get URL from queue

                if not isinstance(job_url, str) or not job_url.startswith("https"):
                    logging.error(f"Invalid data in download queue: {job_url}")
                    download_queue.task_done()
                    continue

                job_html = self.fetcher.fetch_page(job_url)

                if not job_html:
                    logging.warning(f"Skipping download, empty HTML for: {job_url}")
                    download_queue.task_done()
                    continue  # Skip to next job

                with self.lock:  # Prevent race conditions
                    if self.num_html_saved >= HTML_FILE_LIMIT:
                        logging.info(f"Download limit reached: {self.num_html_saved}/{HTML_FILE_LIMIT}")
                        break  # Stop downloading if limit reached

                    self.num_html_saved += 1  # Safe counter increment
                    file_index = self.num_html_saved  # Assign file index

                # Save HTML file
                html_filename = os.path.join(self.local_html_folder, f"job_{file_index}.html")
                with open(html_filename, "w", encoding="utf-8") as f:
                    f.write(job_html)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure file is written to disk

                #logging.info(f"Saved HTML file: {html_filename}")
                self.progress.update_download()

                download_queue.task_done()

            except queue.Empty:
                logging.info("Download queue is empty, stopping.")
                break  # **Exit loop when queue is empty**

        logging.info(f"Saved {self.num_html_saved}/{HTML_FILE_LIMIT} files.")
        download_queue.join()
        with self.lock:
            self.progress.downloading_done = True

        logging.info("Downloading process finished.")

    def zip_html_files(self):
        """
        Zips all downloaded HTML files into a single archive when limit is reached.
        """
        if self.num_html_saved < HTML_FILE_LIMIT:
            logging.info("Not enough HTML files to zip yet.")
            return

        output_zip = os.path.join(DATA_FOLDER, "job_html_files.zip")

        try:
            with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
                for root, _, files in os.walk(self.local_html_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arc_name = os.path.relpath(file_path, start=self.local_html_folder)
                        zip_file.write(file_path, arcname=arc_name)
                        logging.info(f"Added {arc_name} to zip")

            logging.info(f"Zipped HTML files to {output_zip}")

        except Exception as err:
            logging.error(f"Error zipping HTML files: {err}")
