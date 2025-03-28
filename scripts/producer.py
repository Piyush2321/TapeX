#This script is been taken from gitlab repo on july 18, 2024

import enum
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import subprocess
import os
import shutil
import json
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import uvicorn

app = FastAPI(title="TapeX API", description="Tape Management System API")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="web"), name="static")
templates = Jinja2Templates(directory="web/templates")

KAFKA_BOOTSTRAP_SERVERS = '192.168.111.19:9092'
LOG_TOPIC = 'tapedrivestorage'

# Pydantic models for request validation
class FileData(BaseModel):
    username: str
    timestamp: str
    client_ip: str
    Filename: str
    Filesize_in_bytes: int
    File_in_or_out: str
    file_destination: str

class FileLog(BaseModel):
    id: int
    username: str
    timestamp: datetime
    client_ip: str
    Filename: str
    Filesize_in_bytes: int
    File_in_or_out: str
    file_destination: str
    Current_location: Optional[str] = None
    Disk: Optional[str] = None
    AlreadyRetrieved: Optional[bool] = None

def serialize_datetime(obj):
    if isinstance(obj, (datetime, enum.Enum)):
        return obj.isoformat() if isinstance(obj, datetime) else obj.value
    raise TypeError("Type not serializable")

async def publish_to_kafka(topic: str, data: dict):
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(topic, json.dumps(data, default=serialize_datetime).encode('utf-8'))
    producer.flush()

async def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='192.168.111.19',
            user='ftp',
            password='Slack@123',
            database='ftp_commands'
        )

        if connection.is_connected():
            print("Connected to MySQL server")
            return connection

    except Error as e:
        print(f"Error: {e}")
        return None

async def insert_into_table(connection, username: str, timestamp: datetime, client_ip: str, 
                          Filesize_in_bytes: int, Filename: str, File_in_or_out: str, 
                          file_destination: str):
    try:
        cursor = connection.cursor()
        Filename = Filename.lstrip('/')
        client_ip = client_ip.lstrip('::ffff:')

        query = """
            INSERT INTO ftp_logs 
            (username, timestamp, client_ip, Filesize_in_bytes, Filename, File_in_or_out, file_destination) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (username, timestamp, client_ip, Filesize_in_bytes, 
                             Filename, File_in_or_out, file_destination))
        connection.commit()
        print("Record inserted successfully")
        cursor.close()

    except Error as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/receive_csv")
async def receive_csv(file_data: FileData):
    try:
        # Convert timestamp string to datetime
        try:
            timestamp = datetime.strptime(file_data.timestamp, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")

        # Insert into database
        connection = await connect_to_mysql()
        if connection:
            await insert_into_table(
                connection, 
                file_data.username, 
                timestamp, 
                file_data.client_ip,
                file_data.Filesize_in_bytes,
                file_data.Filename,
                file_data.File_in_or_out,
                file_data.file_destination
            )
            
            # Publish to Kafka
            await publish_to_kafka(LOG_TOPIC, file_data.dict())
            
            connection.close()
            return {"success": True, "message": "Data processed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Database connection failed")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/")
async def search_files(username: str = Form(...)):
    username = username.split('@')[0]
    connection = await connect_to_mysql()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = connection.cursor(dictionary=True)
        query = """
            SELECT id, username, timestamp, client_ip, Filename, Filesize_in_bytes, 
                   File_in_or_out, file_destination, Current_location, Disk, AlreadyRetrieved 
            FROM ftp_logs 
            WHERE username = %s
        """
        cursor.execute(query, (username,))
        ftp_logs = cursor.fetchall()
        
        if ftp_logs:
            return {"success": True, "logs": ftp_logs}
        else:
            return {"success": False, "message": "No records found for the specified username."}
    finally:
        cursor.close()
        connection.close()

@app.post("/download")
async def check_download(filename: str = Form(...), username: str = Form(...)):
    extracted_filename = filename.split('/')[-1]
    default_directory = f'/home/octro/var/ftp_home/{username}/fetchedfiles/'
    
    if os.path.exists(os.path.join(default_directory, extracted_filename)):
        return {"result": "File is currently in your default directory"}
    else:
        return {"result": "Schedule Download this file"}

@app.post("/extract")
async def extract(log_id: str = Form(...)):
    try:
        await kafka_producer(log_id)
        return {"success": True, "message": "Download scheduled successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def kafka_producer(log_id: str):
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    try:
        connection = await connect_to_mysql()
        if connection:
            cursor = connection.cursor(dictionary=True)
            query = """
                SELECT id, username, timestamp, client_ip, Filename, Filesize_in_bytes, 
                       File_in_or_out, file_destination 
                FROM ftp_logs 
                WHERE id = %s
            """
            cursor.execute(query, (log_id,))
            rows = cursor.fetchall()
            
            for row in rows:
                if row["File_in_or_out"] == "into server":
                    row["File_in_or_out"] = "out from server"
                producer.produce(LOG_TOPIC, json.dumps(row, default=serialize_datetime).encode('utf-8'), 
                              callback=delivery_report)
            producer.flush()
    except Error as e:
        print(f"Error fetching data from MySQL: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()

if __name__ == "__main__":
    uvicorn.run(app, host='65.49.70.218', port=3000)

