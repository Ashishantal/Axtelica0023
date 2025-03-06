from kafka import KafkaConsumer # type: ignore
import mysql.connector # type: ignore
import json
import time

# MySQL Configuration
DB_CONFIG = {
   "host": "localhost",
    "user": "Ashishdb",
    "password": "1234",
    "database": "kafka_data"
}

# Kafka Configuration
KAFKA_TOPIC = "csv_data"
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Function to get MySQL connection
def get_mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Batch Insert Function
def batch_insert():
    batch_size = 150  # Number of rows to insert in one batch
    messages = []  # Buffer for batch
    table_name = None  # Store table name

    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        for message in consumer:
            data = message.value
            if "table_name" not in data:
                print("Skipping record without table_name field")
                continue

            table_name = data.pop("table_name")  # Get table name
            columns = list(data.keys())  # Column names
            values = tuple(data.values())  # Values to insert

            messages.append(values)  # Add row to batch

            # When batch size is reached, insert data
            if len(messages) >= batch_size:
                insert_into_mysql(conn, cursor, table_name, columns, messages)
                messages.clear()  # Reset batch after insert

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()

# Function to insert batch into MySQL
def insert_into_mysql(conn, cursor, table_name, columns, batch_data):
    try:
        column_names = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

        cursor.executemany(insert_query, batch_data)  # ✅ Batch Insert
        conn.commit()
        print(f"Inserted {len(batch_data)} rows into {table_name}")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        conn.rollback()  # Rollback if error happens

if __name__ == "__main__":
    print("Kafka Consumer is running...")
    batch_insert()
