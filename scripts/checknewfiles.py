#!/usr/bin/env python3

import os
import time
import json
import requests
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('checknewfiles.log'),
        logging.StreamHandler()
    ]
)

# Configuration
BASE_DIR = '/var/ftp_home'
CATEGORIES = ['analytics', 'art', 'devops']
PRODUCER_URL = 'http://localhost:3000/receive_csv'

class FileHandler(FileSystemEventHandler):
    def __init__(self, category):
        self.category = category
        self.processed_files = set()
        self.load_processed_files()

    def load_processed_files(self):
        try:
            with open(f'processed_files_{self.category}.txt', 'r') as f:
                self.processed_files = set(line.strip() for line in f)
        except FileNotFoundError:
            self.processed_files = set()

    def save_processed_files(self):
        with open(f'processed_files_{self.category}.txt', 'w') as f:
            for file in self.processed_files:
                f.write(f"{file}\n")

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        filename = os.path.basename(file_path)
        
        # Skip if file was already processed
        if filename in self.processed_files:
            return

        try:
            # Get file metadata
            file_size = os.path.getsize(file_path)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            # Prepare data for producer
            data = {
                'username': 'system',  # Default username for system operations
                'timestamp': file_time.strftime('%Y-%m-%d %H:%M:%S'),
                'client_ip': '127.0.0.1',
                'Filesize_in_bytes': file_size,
                'Filename': filename,
                'File_in_or_out': 'into server',
                'file_destination': self.category
            }

            # Send data to producer
            response = requests.post(PRODUCER_URL, json=data)
            
            if response.status_code == 200:
                logging.info(f"Successfully processed new file: {filename}")
                self.processed_files.add(filename)
                self.save_processed_files()
            else:
                logging.error(f"Failed to process file {filename}: {response.text}")

        except Exception as e:
            logging.error(f"Error processing file {filename}: {str(e)}")

def setup_observers():
    observers = []
    for category in CATEGORIES:
        path = os.path.join(BASE_DIR, category)
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Created directory: {path}")

        event_handler = FileHandler(category)
        observer = Observer()
        observer.schedule(event_handler, path, recursive=False)
        observer.start()
        observers.append(observer)
        logging.info(f"Started monitoring directory: {path}")

    return observers

def main():
    logging.info("Starting file monitoring service")
    observers = setup_observers()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping file monitoring service")
        for observer in observers:
            observer.stop()
            observer.join()

if __name__ == "__main__":
    main() 