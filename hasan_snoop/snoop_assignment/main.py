import json
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import psycopg2.extras
import uuid

def load_data_from_json(file_path):
    """Loads data from a JSON file into a DataFrame."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data['transactions'])

def connect_to_database():
    """Connects to the PostgreSQL database."""
    try:
        psycopg2.extras.register_uuid()
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Connected to PostgreSQL database successfully!")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error connecting to database:", error)
        return None

def create_tables(cursor):
    """Creates tables in the PostgreSQL database."""
    try:
        # Define the SQL command to create the 'transactions_table'
        sql_create_transactions_table = """
            CREATE TABLE IF NOT EXISTS transactions_table (
                customerId UUID,
                customerName VARCHAR(255),
                transactionId UUID,
                transactionDate DATE,
                sourceDate TIMESTAMP,
                merchantId INTEGER,
                categoryId INTEGER,
                currency VARCHAR(3),
                amount NUMERIC(10, 2),
                description VARCHAR(255),
                PRIMARY KEY (customerId, transactionId)
            );
        """

        # Define the SQL command to create the 'customer_table'
        sql_create_customer_table = """
            CREATE TABLE IF NOT EXISTS customer_table (
                customerId UUID,
                customerName VARCHAR(255),
                transactionDate  DATE,
                PRIMARY KEY (customerId)
            );
        """

        # Define the SQL command to create the 'error_log_table'
        sql_create_error_log_table = """
            CREATE TABLE IF NOT EXISTS error_log_table (
                customerId UUID,
                transactionId UUID, 
                description VARCHAR(255)
            );
        """

        # Execute the SQL commands to create tables
        cursor.execute(sql_create_transactions_table)
        cursor.execute(sql_create_customer_table)
        cursor.execute(sql_create_error_log_table)
        print("Tables created successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error creating tables:", error)

def data_quality_checks(df):
    """Performs data quality checks on the DataFrame."""
    allowable_currencies = ['EUR', 'GBP', 'USD']

    # Condition 1: Validate the currency against the allowable currency list
    condition1 = df['currency'].isin(allowable_currencies)

    # Condition 2: Check for invalid transactionDate (not in YYYY-MM-DD format)
    condition2 = pd.to_datetime(df['transactionDate'], format='%Y-%m-%d', errors='coerce').notna()

    # Condition 3: Check for duplicate transaction records (based on transactionId and customerId)
    condition3 = df.groupby(['transactionId', 'customerId'])['transactionId'].transform('nunique') == 1

    # Combine the conditions using boolean operators 
    final_condition = condition1 & condition2 & condition3

    # Filter the DataFrame using the final condition
    filtered_df = df[final_condition]

    # Combine the conditions using boolean operators
    failed_condition = (~condition1) | (~condition2) | (~condition3)

    # Filter the DataFrame using the failed condition
    failed_df = df[failed_condition]

    return filtered_df, failed_df

def insert_into_transactions_table(cursor, filtered_df):
    """Inserts or updates records into the transactions_table."""
    try:
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO transactions_table (
                customerId,
                customerName,
                transactionId,
                transactionDate,
                sourceDate,
                merchantId,
                categoryId,
                currency,
                amount,
                description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customerId, transactionId) DO UPDATE
            SET customerName = EXCLUDED.customerName,
                transactionDate = EXCLUDED.transactionDate,
                sourceDate = EXCLUDED.sourceDate,
                merchantId = EXCLUDED.merchantId,
                categoryId = EXCLUDED.categoryId,
                currency = EXCLUDED.currency,
                amount = EXCLUDED.amount,
                description = EXCLUDED.description
            """,
            [tuple(row) for row in filtered_df.itertuples(index=False, name=None)],
        )
        print("Transactions inserted or updated successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error inserting or updating transactions:", error)

def insert_into_customer_table(cursor, filtered_df):
    """Inserts or updates records into the customer_table."""
    try:
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO customer_table (
                customerId,
                customerName,
                transactionDate 
            ) VALUES (%s, %s, %s)
            ON CONFLICT (customerId) DO UPDATE
            SET customerName = EXCLUDED.customerName,
                transactionDate  = GREATEST(EXCLUDED.transactionDate , customer_table.transactionDate )
            """,
            [tuple(row) for row in filtered_df[['customerId', 'customerName', 'transactionDate']].itertuples(index=False, name=None)],
        )
        print("Customer records inserted or updated successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error inserting or updating customer records:", error)

def insert_into_error_log_table(cursor, failed_df):
    """Inserts records into the error_log_table."""
    try:
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO error_log_table (
                customerId,
                transactionId,
                description
            ) VALUES (%s, %s, %s)
            """,
            [tuple(row) for row in failed_df[['customerId', 'transactionId', 'description']].itertuples(index=False, name=None)],
        )
        print("Error log records inserted successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error inserting error log records:", error)

def main():
    load_dotenv()  # Load environment variables

    file_path = os.getenv("FILE_PATH")
    df = load_data_from_json(file_path)

    conn = connect_to_database()

    if conn:
        try:
            with conn.cursor() as cursor:
                create_tables(cursor)

                filtered_df, failed_df = data_quality_checks(df)

                # Data type conversions for the DataFrame to get into form to load into tables
                filtered_df.loc[:, 'sourceDate'] = pd.to_datetime(filtered_df['sourceDate'])
                filtered_df.loc[:, 'amount'] = pd.to_numeric(filtered_df['amount'])
                filtered_df.loc[:, 'customerId'] = filtered_df['customerId'].apply(uuid.UUID)
                filtered_df.loc[:, 'transactionId'] = filtered_df['transactionId'].apply(uuid.UUID)
                filtered_df.loc[:, 'transactionDate'] = pd.to_datetime(filtered_df['transactionDate'])

                insert_into_transactions_table(cursor, filtered_df)
                insert_into_customer_table(cursor, filtered_df)
                insert_into_error_log_table(cursor, failed_df)

            conn.commit()
        finally:
            conn.close()

if __name__ == "__main__":
    main()
