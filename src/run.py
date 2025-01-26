from datetime import datetime, timedelta
from src.api import fetch_historical_data
import pandas as pd
import psycopg2
from psycopg2 import sql

# Read CSV to get instrument keys
df = pd.read_csv(r'ind_nifty50list.csv')

# Date range setup
end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')

# Database connection parameters
db_params = {
    'dbname': 'postgres',  # Initially connect to default 'postgres' database
    'user': 'postgres',
    'password': 'Lexicon#11',
    'host': 'daily-data-fetch.c7m8wwkmaj1u.ap-southeast-2.rds.amazonaws.com',
    'port': '5432'
}

# Establish initial connection to PostgreSQL to create the database if not exists
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Check if the target database exists
cursor.execute("""
    SELECT 1 FROM pg_database WHERE datname = 'daily-data-fetch';
""")
if cursor.fetchone() is None:
    # If database does not exist, create it
    cursor.close()  # Close the cursor before creating the database
    conn.close()    # Close the current connection

    # Create a new connection to the default database
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="Lexicon#11", host="daily-data-fetch.c7m8wwkmaj1u.ap-southeast-2.rds.amazonaws.com", port="5432")
    cursor = conn.cursor()

    cursor.execute('CREATE DATABASE "daily-data-fetch";')
    print("Database 'daily-data-fetch' created successfully!")

    cursor.close()  # Close the cursor after creating the database
    conn.close()    # Close the connection

# Reconnect to the newly created database
db_params['dbname'] = 'daily-data-fetch'
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Check if the required table exists
cursor.execute("""
    SELECT to_regclass('public.your_table_name');
""")
if cursor.fetchone()[0] is None:
    # Create table if it does not exist
    create_table_query = """
    CREATE TABLE your_table_name (
        date DATE,
        instrument_key VARCHAR(50),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INTEGER
    );
    """
    cursor.execute(create_table_query)
    print("Table 'your_table_name' created successfully!")

# Loop through each instrument and fetch data
for instrument_key in df['instrument_key']:

    df_current_period = fetch_historical_data(instrument_key, start_date, end_date)

    # If data is found, insert it into PostgreSQL
    if not df_current_period.empty:
        # Iterate through the rows and insert them into the database
        for index, row in df_current_period.iterrows():
            # Assuming 'date', 'instrument_key', 'open', 'high', 'low', 'close', 'volume' columns exist
            insert_query = sql.SQL("""
                INSERT INTO your_table_name (date, instrument_key, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (row['date'], row['instrument_key'], row['open'], row['high'], row['low'], row['close'], row['volume']))

        # Commit the transaction
        conn.commit()
    
    print(f"Data for {instrument_key} inserted successfully!")

# Close the cursor and connection
cursor.close()
conn.close()
