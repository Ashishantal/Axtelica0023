from flask import Flask, render_template, request, redirect, url_for # type: ignore
import pandas as pd # type: ignore
import mysql.connector # type: ignore
import os
from kafka import KafkaProducer, KafkaAdminClient # type: ignore
from kafka.admin import NewTopic # type: ignore
import json

app = Flask(__name__)

# MySQL Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "Ashishdb",
    "password": "1234",
    "database": "kafka_data"
}

# Kafka Configuration
KAFKA_TOPIC = "csv_data"
KAFKA_BROKER = "localhost:9092"

# Create Kafka topic if not exists
def create_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    existing_topics = admin_client.list_topics()
    if KAFKA_TOPIC not in existing_topics:
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    admin_client.close()

create_kafka_topic()

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Connect to MySQL
def get_mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Fetch all table names
def get_tables():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    conn.close()
    return tables

@app.route("/")
def index():
    tables = get_tables()
    return render_template("index.html", tables=tables)

#show total no. of row def get_row_count():
@app.route("/get_row_count", methods=["POST"])
def get_row_count():
    file = request.files["file"]
    
    df = pd.read_csv(file)
    return len(df)

@app.route("/upload", methods=["POST"])
def upload_csv():
    file = request.files["file"]
    if not file:
        return redirect(url_for("index"))

    df = pd.read_csv(file)
    table_name = os.path.splitext(file.filename)[0]

    conn = get_mysql_connection()
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    # Create table if it does not exist
    if not table_exists:
        columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
        create_query = f"CREATE TABLE `{table_name}` ({columns})"
        cursor.execute(create_query)

    conn.commit()
    cursor.close()
    conn.close()

    # Send data to Kafka
    for _, row in df.iterrows():
        data = row.to_dict()
        data["table_name"] = table_name
        producer.send(KAFKA_TOPIC, data)

    return redirect(url_for("index"))

@app.route("/insert", methods=["POST"])
def insert_data():
    table_name = request.form["table"]
    file = request.files["file"]

    if not file or table_name not in get_tables():
        return redirect(url_for("index"))

    df = pd.read_csv(file)

    # Check table structure
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    table_columns = {col[0] for col in cursor.fetchall()}
    csv_columns = set(df.columns)

    if table_columns != csv_columns:
        return redirect(url_for("index"))

    # Send data to Kafka
    for _, row in df.iterrows():
        data = row.to_dict()
        data["table_name"] = table_name
        producer.send(KAFKA_TOPIC, data)

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True)
