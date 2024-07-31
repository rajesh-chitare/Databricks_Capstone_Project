import pandas as pd
import requests
import base64
import logging
import os
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Databricks connection details
databricks_instance = "https://adb-1971747938457049.9.azuredatabricks.net"  # Your Databricks instance URL
databricks_token = "dapi6e5917bca15a0df25c3d758f3c5abb57"  # Your Databricks personal access token
dbfs_directory = "dbfs:/FileStore/Databricks_Capstone_Rajesh/Datasets/Transactions_batch_file"  # Directory in DBFS
input_file_path = r"D:\Databricks\Capstone Project\Datasets\transactions.csv"  # Path to input file
checkpoint_file = r"D:\Databricks\Capstone Project\Datasets\upload_checkpoint.txt"  # Checkpoint file

def read_checkpoint():
    """Read the checkpoint file and return a set of uploaded file names."""
    if not os.path.exists(checkpoint_file):
        return set()
    with open(checkpoint_file, 'r') as f:
        return set(line.strip() for line in f)

def write_checkpoint(uploaded_files):
    """Write the list of uploaded files to the checkpoint file."""
    with open(checkpoint_file, 'a') as f:
        for file_name in uploaded_files:
            f.write(f"{file_name}\n")

def upload_to_dbfs(content, dbfs_path):
    """Upload the file content to DBFS."""
    b64_encoded_content = base64.b64encode(content).decode('utf-8')
    url = f"{databricks_instance}/api/2.0/dbfs/put"
    headers = {
        "Authorization": f"Bearer {databricks_token}"
    }
    data = {
        "path": dbfs_path,
        "contents": b64_encoded_content,
        "overwrite": True
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    logging.info(f"Uploaded to {dbfs_path}")

def process_batches():
    """Read the CSV file in batches and upload each batch to DBFS."""
    batch_size = 50
    uploaded_files = read_checkpoint()

    for batch_num, chunk_df in enumerate(pd.read_csv(input_file_path, chunksize=batch_size)):
        # Prepare the batch content
        batch_csv = chunk_df.to_csv(index=False)
        batch_file_name = f"transactions_batch_{batch_num + 1}.csv"
        dbfs_path = os.path.join(dbfs_directory, batch_file_name)
        
        if batch_file_name not in uploaded_files:
            try:
                upload_to_dbfs(batch_csv.encode('utf-8'), dbfs_path)
                write_checkpoint([batch_file_name])
            except Exception as e:
                logging.error(f"Failed to upload batch {batch_num + 1}: {e}")

if __name__ == "__main__":
    logging.info("Processing batches...")
    process_batches()
    logging.info("Processing completed.")
