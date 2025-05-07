import os
import time
import csv
import json
import duckdb
import logging
import threading
import queue
from config import DATA_FOLDER

class DataSaver:
    """
    Saves scraped job data to CSV, JSON, and SQLite.
    Uses a queue-based system and batch processing for efficiency.
    """

    def __init__(self):
        """
        Initializes the data saver with a queue system.
        """
        self.job_queue = queue.Queue()  # Job queue for saving data
        self.db_path = os.path.join(DATA_FOLDER, "jobs.duckdb")
        self.csv_path = os.path.join(DATA_FOLDER, "job_descriptions.csv")
        self.json_path = os.path.join(DATA_FOLDER, "job_descriptions.json")
        self.seen_jobs = set()  # Track saved job URLs to prevent duplicates
        self.lock = threading.Lock()  # Ensure thread safety
        self.header_written = False  # Track if the header has been written
        self.queue_empty = False  # Prevent duplicate saving

        # Ensure storage folder exists
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)

        # Start the background saving thread
        self.saving_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.saving_thread.start()

    def save_job(self, job_data):
        """
        Adds job data to the queue for saving.
        Prevents duplicate job entries.
        """
        job_url = job_data["url"]

        with self.lock:  # Prevent race conditions
            if job_url in self.seen_jobs:
                logging.warning(f"Duplicate job skipped: {job_url}")
                return
            self.seen_jobs.add(job_url)  # Track saved jobs
        self.job_queue.put(job_data)  # Add job to queue

    def process_queue(self):
        """
        Continuously processes and saves jobs from the queue in batches.
        Runs in a separate thread to prevent blocking.
        """
        batch_size = 50  # Save in batches of 50 jobs for efficiency
        batch = []
        last_save_time = time.time()  # Track last save time

        while True:
            try:
                job = self.job_queue.get(timeout=10)  # Wait for job data
                batch.append(job)

                # When batch size is reached, save all at once
                if len(batch) >= batch_size:
                    self.batch_save_jobs(batch)
                    batch.clear()  # Reset batch
                    last_save_time = time.time()  # Update last save time

                self.job_queue.task_done()  # Mark as processed


            except queue.Empty:
                # If there are remaining jobs and no new ones for 10s, save them
                if batch and (time.time() - last_save_time >= 10):
                    self.batch_save_jobs(batch)
                    batch.clear()  # Reset batch after final save
                    logging.info("Final batch saved after timeout.")
                    break  # Exit loop when all jobs are saved

    def batch_save_jobs(self, job_list):
        """
        Saves a batch of job data at once to CSV, JSON, and SQLite.
        """
        if not job_list:
            logging.warning("No job data available to save.")
            return

        self.save_to_csv(job_list)
        self.save_to_json(job_list)
        self.save_to_duckdb(job_list)
        #logging.info(f"Saved {len(job_list)} job entries successfully.")

    def save_to_csv(self, job_list):
        """Saves job data to a CSV file."""
        if not job_list:
            logging.warning("No job data to save.")
            return

        fields = [
            "id", "url", "title", "publication_date", "workload",
            "contract_type", "salary", "languages", "place_of_work"
        ]

        with self.lock:  # Prevent multiple threads from writing simultaneously
            try:
                # Check if the file is empty
                file_empty = not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0

                with open(self.csv_path, mode="a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields)

                    # Write the header **ONLY IF** the file is empty
                    if file_empty:
                        writer.writeheader()

                    # Write job data
                    for job in job_list:
                        writer.writerow({field: job.get(field, "N/A") for field in fields})

                logging.info(f"Saved {len(job_list)} jobs to CSV: {self.csv_path}")


            except Exception as e:
                logging.error(f"Error saving to CSV: {e}")

    def save_to_json(self, job_list):
        """Saves job data to a JSON file safely in batches."""
        if not job_list:
            logging.warning("No job data to save.")
            return

        try:
            # Read existing JSON, but safely handle corrupt files
            if os.path.exists(self.json_path):
                try:
                    with open(self.json_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except json.JSONDecodeError:
                    logging.warning("Corrupt JSON file detected. Resetting file.")
                    data = []  # Reset if corrupt
            else:
                data = []

            data.extend(job_list)  # Append new jobs

            # Save the updated list to JSON
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            logging.info(f"Saved {len(job_list)} jobs to JSON: {self.json_path}")

        except Exception as e:
            logging.error(f"Error saving to JSON: {e}")

    def save_to_duckdb(self, job_list):
        """Saves job data to a DuckDB database."""
        try:
            conn = duckdb.connect(self.db_path)  # e.g. self.db_path = "jobs.duckdb"
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_descriptions (
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    publication_date TEXT,
                    workload TEXT,
                    contract_type TEXT,
                    salary TEXT,
                    languages TEXT,
                    place_of_work TEXT
                )
            ''')

            cursor.executemany('''
                INSERT INTO job_descriptions 
                (title, url, publication_date, workload, contract_type, salary, languages, place_of_work)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', [(job["title"], job["url"], job.get("publication_date", "N/A"),
                   job.get("workload", "N/A"), job.get("contract_type", "N/A"),
                   job.get("salary", "N/A"), job.get("languages", "N/A"), job.get("place_of_work", "N/A"))
                  for job in job_list])

            conn.close()
            logging.info(f"Saved {len(job_list)} jobs to DuckDB database.")
        except Exception as e:
            logging.error(f"Error saving to DuckDB: {e}")