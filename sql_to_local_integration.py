import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

def main():
    try:
        # MySQL connection properties
        hostname = "127.0.0.1"  # Use IP address instead of 'localhost'
        port = "3306"
        database = "bank_service"  # Replace with your database name
        username = "root"  # Replace with your MySQL username
        password = "Rajesh@123"  # Replace with your MySQL password

        # Encode special characters in the password
        password = urllib.parse.quote_plus(password)

        # Create the MySQL connection engine with explicit format
        connection_string = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database}"
        engine = create_engine(connection_string)

        # List of tables to read
        tables = ["branches", "customers"]

        # Read each table and store it locally as a CSV file
        for table in tables:
            try:
                # Read data from MySQL into a DataFrame
                df = pd.read_sql_table(table, con=engine)

                # Save DataFrame to a CSV file
                path = r"D:\Databricks\Capstone Project\dataset_from_sql"
                local_path = os.path.join(path, f"{table}.csv")
                df.to_csv(local_path, index=False)
                print(f"{table} table stored in {path}")

            except Exception as e:
                print(f"An error occurred while processing table {table}: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
