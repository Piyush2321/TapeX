#!/usr/bin/env python3

import json
import os
import time
import logging
import mysql.connector
from mysql.connector import Error
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer.log'),
        logging.StreamHandler()
    ]
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = '192.168.111.19:9092'
KAFKA_TOPIC = 'tapedrivestorage'
KAFKA_GROUP_ID = 'tape_consumer_group'

# Database configuration
DB_CONFIG = {
    'host': '192.168.111.19',
    'user': 'ftp',
    'password': 'Slack@123',
    'database': 'ftp_commands'
}

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logging.info("Connected to MySQL server")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def update_file_location(connection, log_id, location, disk=None):
    try:
        cursor = connection.cursor()
        query = """
            UPDATE ftp_logs 
            SET Current_location = %s, Disk = %s 
            WHERE id = %s
        """
        cursor.execute(query, (location, disk, log_id))
        connection.commit()
        logging.info(f"Updated file location for log_id {log_id} to {location}")
    except Error as e:
        logging.error(f"Error updating file location: {e}")
    finally:
        cursor.close()

def move_to_tape(file_path, tape_drive):
    """
    Simulate moving a file to tape storage.
    In a real implementation, this would use actual tape drive commands.
    """
    try:
        # Here you would implement actual tape drive operations
        # For now, we'll just simulate the operation
        time.sleep(2)  # Simulate tape operation time
        logging.info(f"File {file_path} moved to tape drive {tape_drive}")
        return True
    except Exception as e:
        logging.error(f"Error moving file to tape: {e}")
        return False

def retrieve_from_tape(file_path, tape_drive):
    """
    Simulate retrieving a file from tape storage.
    In a real implementation, this would use actual tape drive commands.
    """
    try:
        # Here you would implement actual tape drive operations
        # For now, we'll just simulate the operation
        time.sleep(2)  # Simulate tape operation time
        logging.info(f"File {file_path} retrieved from tape drive {tape_drive}")
        return True
    except Exception as e:
        logging.error(f"Error retrieving file from tape: {e}")
        return False

def process_message(msg):
    try:
        data = json.loads(msg.value().decode('utf-8'))
        log_id = data.get('id')
        filename = data.get('Filename')
        file_destination = data.get('file_destination')
        file_in_or_out = data.get('File_in_or_out')

        if not all([log_id, filename, file_destination, file_in_or_out]):
            logging.error("Missing required fields in message")
            return

        connection = connect_to_mysql()
        if not connection:
            return

        try:
            if file_in_or_out == 'into server':
                # Move file to tape storage
                file_path = os.path.join('/var/ftp_home', file_destination, filename)
                if os.path.exists(file_path):
                    tape_drive = 'TAPE1'  # In a real implementation, you would manage multiple tape drives
                    if move_to_tape(file_path, tape_drive):
                        update_file_location(connection, log_id, 'tape', tape_drive)
                        # In a real implementation, you would delete the file from disk after successful tape storage
                        # os.remove(file_path)
                    else:
                        logging.error(f"Failed to move file {filename} to tape")
                else:
                    logging.error(f"File {filename} not found at {file_path}")

            elif file_in_or_out == 'out from server':
                # Retrieve file from tape storage
                tape_drive = 'TAPE1'  # In a real implementation, you would get this from the database
                file_path = os.path.join('/var/ftp_home', file_destination, filename)
                if retrieve_from_tape(file_path, tape_drive):
                    update_file_location(connection, log_id, 'disk', None)
                else:
                    logging.error(f"Failed to retrieve file {filename} from tape")

        finally:
            connection.close()

    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    # Configure Kafka consumer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    logging.info("Starting Kafka consumer")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                    break

            process_message(msg)

    except KeyboardInterrupt:
        logging.info("Stopping consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
