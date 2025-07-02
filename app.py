from flask import Flask, render_template, request, redirect, url_for, flash  # type: ignore
import pandas as pd # type: ignore
from io import StringIO

import os
from dotenv import load_dotenv # type: ignore
from kafka import KafkaProducer, KafkaAdminClient # type: ignore
from kafka.admin import NewTopic # type: ignore
import json
import importlib
import logging
from logging.handlers import RotatingFileHandler
import boto3 # type: ignore
import mysql.connector.pooling as mysql_pooling # type: ignore
import traceback

load_dotenv()  # Load environment variables from .env file

# S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")

logging.info(f"{S3_BUCKET_NAME=}")
logging.info(f"{S3_REGION=}")
logging.info(f"{AWS_ACCESS_KEY_ID=}")
logging.info(f"{AWS_SECRET_ACCESS_KEY=}")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    region_name=S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'  # Replace with a strong, random key

# Reduce logging level
logging.getLogger("kafka").setLevel(logging.WARNING)

# Create log directory if it doesn't exist
if not os.path.exists('log'):
    os.makedirs('log')

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    'log/app.log',
    maxBytes=1024 * 1024,  # 1MB
    backupCount=5
)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.INFO)

logging.getLogger().addHandler(log_handler)

# Database Configuration
DB_TYPE = os.getenv("DB_TYPE", "mysql")  # Default to mysql
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Dynamically load the database driver
try:
    if DB_TYPE == "mysql":
        import mysql.connector.pooling as db_pooling # type: ignore
        db_module = "mysql.connector"
    elif DB_TYPE == "postgresql":
        import psycopg2.pool as db_pooling # type: ignore
        db_module = "psycopg2"
    elif DB_TYPE == "sqlite":
        import sqlite3 as db_pooling
        db_module = "sqlite3"
    else:
        raise ValueError("Invalid DB_TYPE. Supported values: mysql, postgresql, sqlite")

    logging.info(f"Using database type: {DB_TYPE}")
except ImportError as e:
    logging.error(f"Error importing database driver: {e}")
    raise

# Database Connection Pool
if DB_TYPE == "sqlite":
    db_pool = None  # SQLite doesn't need a connection pool
else:
    DB_CONFIG = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_DATABASE
    }
    db_pool = db_pooling.MySQLConnectionPool(  # Changed to generic name
        pool_name="mypool",
        pool_size=5,
        **DB_CONFIG
    )

# Function to get database connection
def get_db_connection():
    conn = None
    try:
        if DB_TYPE == "sqlite":
            conn = db_pooling.connect(database=DB_DATABASE)
        else:
            conn = db_pool.get_connection()
        logging.info("Connected to database")
        return conn
    except Exception as e:
        logging.exception(f"Error connecting to database: {e}")
        return None

# Fetch all table names
def get_tables():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

# Fetch row count for a table
def get_row_count(table_name):
    conn = get_db_connection()
    if not conn:
        return 0
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count

# Kafka Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Batch Size Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))  # Number of rows to send in one batch

# Create Kafka topic if not exists
def create_kafka_topic():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        existing_topics = admin_client.list_topics()
        if KAFKA_TOPIC not in existing_topics:
            topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
            logging.info(f"Kafka topic '{KAFKA_TOPIC}' created")
        admin_client.close()
    except Exception as e:
        logging.error(f"Error creating Kafka topic: {e}")

# Create Kafka topic
try:
    create_kafka_topic()
except Exception as e:
    logging.error(f"Error creating Kafka topic: {e}")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",  # Ensure messages are acknowledged by all replicas
    retries=3,  # Retry failed messages
    linger_ms=100  # Wait up to 100ms to batch messages
)
 
# Function to get database connection
def get_db_connection():
    try:
        if DB_TYPE == "sqlite":
            conn = db_pooling.connect(database=DB_DATABASE)
        else:
            conn = db_pool.get_connection()
        logging.info("Connected to database")
        return conn
    except Exception as e:
        logging.exception(f"Error connecting to database: {e}")
        return None

# Fetch all table names
def get_tables():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

# Fetch row count for a table
def get_row_count(table_name):
    conn = get_db_connection()
    if not conn:
        return 0
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count

# Home route
@app.route("/")
def index():
    folders = list_folders_in_s3()
    tables = get_tables()
    table_data = []
    for table in tables:
        row_count = get_row_count(table)
        table_data.append({"name": table, "table_name": table, "row_count": row_count})
    return render_template("index.html", tables=table_data, files=get_s3_files(), folders=folders)

@app.route("/list_s3_files")
def list_s3_files():
    files = get_s3_files()
    file_names = [file['name'] for file in files]
    return json.dumps(file_names)
##########################################################################
@app.route("/list_folders_in_s3")
# List "folders" (common prefixes) from S3
def list_folders_in_s3():
    
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Delimiter='/')
    folders = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
    return folders


@app.route('/upload_folder', methods=['POST'])
def upload_folder():
    try:
        folder_prefix = request.form["folder_prefix"]
        if not folder_prefix:
            flash("No folder selected from S3.", "error")
            return redirect(url_for("index"))

        # List all CSV files under selected folder
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=folder_prefix)
            csv_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

            if not csv_files:
                flash("No CSV files found in the selected folder.", "error")
                return redirect(url_for("index"))
        except Exception as e:
            logging.error(f"Error listing folder contents from S3: {e}")
            flash("Error accessing folder contents. Please check your S3 configuration.", "error")
            return redirect(url_for("index"))

        total_row_count = 0

        for s3_file in csv_files:
            try:
                # Download each CSV file
                obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_file)
                csv_string = obj['Body'].read().decode('utf-8')
                file = StringIO(csv_string)
                df = pd.read_csv(file)
            except Exception as e:
                logging.error(f"Error downloading file {s3_file} from S3: {e}")
                flash(f"Error downloading {s3_file}. Skipping.", "error")
                continue

            table_name = folder_prefix.rstrip('/').replace('/', '_')  # Clean folder name for table

            conn = get_db_connection()
            if not conn:
                flash("Database connection error. Please try again later.", "error")
                return redirect(url_for("index"))
            cursor = conn.cursor()

            # Check if table exists
            try:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                table_exists = cursor.fetchone()

                # Create table if not exists
                if not table_exists:
                    columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
                    create_query = f"CREATE TABLE `{table_name}` ({columns})"
                    cursor.execute(create_query)
                    conn.commit()
                    logging.info(f"Table '{table_name}' created")
            except Exception as e:
                logging.error(f"Error creating or checking table: {e}")
                flash("Error creating or checking table. Please try again later.", "error")
                return redirect(url_for("index"))
            finally:
                cursor.close()
                conn.close()

            # Send data to Kafka in batches
            try:
                batch = []
                for _, row in df.iterrows():
                    data = row.to_dict()
                    data["table_name"] = table_name
                    batch.append(data)
                    total_row_count += 1

                    if len(batch) >= BATCH_SIZE:
                        for record in batch:
                            producer.send(KAFKA_TOPIC, record)
                        producer.flush()
                        batch.clear()

                if batch:
                    for record in batch:
                        producer.send(KAFKA_TOPIC, record)
                    producer.flush()

                logging.info(f"Data from {s3_file} sent to Kafka successfully")
            except Exception as e:
                logging.error(f"Error sending data from {s3_file} to Kafka: {e}")
                flash(f"Error sending data from {s3_file}.", "error")
                continue

        flash(f"Folder uploaded from S3 and data sent successfully. {total_row_count} rows sent to Kafka.", "success")
        logging.info("Folder uploaded from S3 and data sent successfully.")
        return redirect(url_for("index"))

    except Exception as e:
        logging.error(f"Unhandled error in /upload_folder: {e}")
        flash("An unexpected error occurred while uploading folder. Please try again later.", "error")
        return redirect(url_for("index"))

###############################################################################
# Upload CSV from S3 route
@app.route("/upload_s3", methods=["POST"])
def upload_s3():
    try:
        s3_file = request.form["s3_file"]
        if not s3_file:
            flash("No file selected from S3.", "error")
            return redirect(url_for("index"))

        # Download file from S3
        try:
            obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_file)
            csv_string = obj['Body'].read().decode('utf-8')
            file = StringIO(csv_string)
            df = pd.read_csv(file)
        except Exception as e:
            logging.error(f"Error downloading file from S3: {e}")
            flash("Error downloading file from S3. Please check your S3 configuration and file name.", "error")
            return redirect(url_for("index"))

        table_name = os.path.splitext(os.path.basename(s3_file))[0]

        conn = get_db_connection()
        if not conn:
            flash("Database connection error. Please try again later.", "error")
            return redirect(url_for("index"))
        cursor = conn.cursor()

        # Check if table exists
        try:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()

            # Create table if it does not exist
            if not table_exists:
                columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
                create_query = f"CREATE TABLE `{table_name}` ({columns})"
                cursor.execute(create_query)
                conn.commit()
                logging.info(f"Table '{table_name}' created")
        except Exception as e:
            logging.error(f"Error creating or checking table: {e}")
            flash("Error creating or checking table. Please try again later.", "error")
            return redirect(url_for("index"))
        finally:
            cursor.close()
            conn.close()

        # Send data to Kafka in batches
        try:
            batch = []
            row_count = 0
            for _, row in df.iterrows():
                data = row.to_dict()
                data["table_name"] = table_name
                batch.append(data)
                row_count += 1

                # Send batch when it reaches the batch size
                if len(batch) >= BATCH_SIZE:
                    for record in batch:
                        producer.send(KAFKA_TOPIC, record)
                    producer.flush()  # Ensure all messages are sent
                    batch.clear()  # Reset the batch

            # Send remaining rows (if any)
            if batch:
                for record in batch:
                    producer.send(KAFKA_TOPIC, record)
                producer.flush()

            logging.info(f"Data sent to Kafka for table '{table_name}' successfully")
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            flash("Error sending data. Please check your data and try again.", "error")
            return redirect(url_for("index"))

        flash(f"File uploaded from S3 and data sent successfully. {row_count} rows sent to Kafka.", "success")
        logging.info("File uploaded from S3 and data sent successfully.")
        return redirect(url_for("index"))

    except Exception as e:
        logging.error(f"Unhandled error in /upload_s3: {e}")
        flash("An unexpected error occurred. Please try again later.", "error")
        return redirect(url_for("index"))

# Function to list files from S3
def get_s3_files():
    try:
        logging.info("Attempting to list files from S3...")
        logging.info(f"Using bucket: {S3_BUCKET_NAME}")
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        logging.info(f"S3 response: {response}")
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'name': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified']
            })
        logging.info(f"Found {len(files)} files in S3.")
        return files
    except Exception as e:
        logging.exception("Error listing files from S3: {traceback.format_exc()}")
        flash("Error listing files from S3. Please check your S3 configuration.", "error")
        return []

# Upload CSV route
@app.route("/upload", methods=["POST"])
def upload_csv():
    try:
        file = request.files["file"]
        if not file:
            flash("No file uploaded.", "error")
            return redirect(url_for("index"))

        try:
            df = pd.read_csv(file)
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            flash("Invalid file format. Please upload a CSV file.", "error")
            return redirect(url_for("index"))

        table_name = os.path.splitext(file.filename)[0]

        conn = get_db_connection()
        if not conn:
            flash("Database connection error. Please try again later.", "error")
            return redirect(url_for("index"))
        cursor = conn.cursor()

        # Check if table exists
        try:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()

            # Create table if it does not exist
            if not table_exists:
                columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
                create_query = f"CREATE TABLE `{table_name}` ({columns})"
                cursor.execute(create_query)
                conn.commit()
                logging.info(f"Table '{table_name}' created")
        except Exception as e:
            logging.error(f"Error creating or checking table: {e}")
            flash("Error creating or checking table. Please try again later.", "error")
            return redirect(url_for("index"))
        finally:
            cursor.close()
            conn.close()

            # Send data to Kafka in batches
            try:
                batch = []
                row_count = 0
                for _, row in df.iterrows():
                    data = row.to_dict()
                    data["table_name"] = table_name
                    batch.append(data)
                    row_count += 1

                    # Send batch when it reaches the batch size
                    if len(batch) >= BATCH_SIZE:
                        for record in batch:
                            producer.send(KAFKA_TOPIC, record)
                        producer.flush()  # Ensure all messages are sent
                        batch.clear()  # Reset the batch

                # Send remaining rows (if any)
                if batch:
                    for record in batch:
                        producer.send(KAFKA_TOPIC, record)
                    producer.flush()

                logging.info(f"Data sent to Kafka for table '{table_name}' successfully")
            except Exception as e:
                logging.error(f"Error sending data to Kafka: {e}")
                flash("Error sending data. Please check your data and try again.", "error")
                return redirect(url_for("index"))

            flash(f"File uploaded and data sent successfully. {row_count} rows sent to Kafka.", "success")
            logging.info("File uploaded and data sent successfully.")
            return redirect(url_for("index"))

    except Exception as e:
        logging.error(f"Unhandled error in /upload: {e}")
        flash("An unexpected error occurred. Please try again later.", "error")
        return redirect(url_for("index"))


# Insert CSV route
@app.route("/insert", methods=["POST"])
def insert_csv():
    try:
        file = request.files["file"]
        table_name = request.form["table"]  # Get the selected table name from the form

        if not file:
            flash("No file uploaded.", "error")
            return redirect(url_for("index"))

        try:
            df = pd.read_csv(file)
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            flash("Invalid file format. Please upload a CSV file.", "error")
            return redirect(url_for("index"))

        # Send data to Kafka in batches
        try:
            batch = []
            row_count = 0
            for _, row in df.iterrows():
                data = row.to_dict()
                data["table_name"] = table_name  # Use the selected table name
                batch.append(data)
                row_count += 1

                # Send batch when it reaches the batch size
                if len(batch) >= BATCH_SIZE:
                    for record in batch:
                        producer.send(KAFKA_TOPIC, record)
                    producer.flush()  # Ensure all messages are sent
                    batch.clear()  # Reset the batch

            # Send remaining rows (if any)
            if batch:
                for record in batch:
                    producer.send(KAFKA_TOPIC, record)
                producer.flush()

            logging.info(f"Data sent to Kafka for table '{table_name}' successfully")
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            flash("Error sending data. Please check your data and try again.", "error")
            return redirect(url_for("index"))

        flash(f"Data inserted into table '{table_name}' successfully. {row_count} rows sent to Kafka.", "success")
        logging.info(f"Data inserted into table '{table_name}' successfully.")
        return redirect(url_for("index"))

    except Exception as e:
        logging.error(f"Unhandled error in /insert: {e}")
        flash("An unexpected error occurred. Please try again later.", "error")
        return redirect(url_for("index"))

# Run the Flask app
if __name__ == "__main__":
    print("Flask app starting...")
    app.run()
