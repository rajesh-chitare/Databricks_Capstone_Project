import os
import requests
import base64

# Databricks connection details
databricks_instance = "https://adb-1971747938457049.9.azuredatabricks.net"  # Your Databricks instance URL
databricks_token = "dapi6e5917bca15a0df25c3d758f3c5abb57"  # Your Databricks personal access token
dbfs_directory = "dbfs:/FileStore/Databricks_Capstone_Rajesh/Datasets"  # Directory in DBFS
local_folder = r"D:\Databricks\Capstone Project\dataset_from_sql"
checkpoint_file = r"D:\Databricks\Capstone Project\Datasets\upload_checkpoint_sql.txt"  # Checkpoint file

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

def upload_files():
    try:
        uploaded_files = read_checkpoint()
        new_uploads = []

        for root, dirs, files in os.walk(local_folder):
            for file_name in files:
                if file_name in uploaded_files:
                    continue  # Skip files that have already been uploaded

                file_path = os.path.join(root, file_name)

                # Get the relative path of the file from the local_folder
                relative_path = os.path.relpath(file_path, local_folder)

                # Prepend the DBFS directory to the relative path
                dbfs_path = os.path.join(dbfs_directory, relative_path)

                # Upload the file to DBFS
                with open(file_path, "rb") as f:
                    file_content = f.read()
                    b64_encoded_content = base64.b64encode(file_content).decode('utf-8')
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

                new_uploads.append(file_name)
                print(f"Uploaded {file_name} to {dbfs_path}")

        if new_uploads:
            write_checkpoint(new_uploads)

    except Exception as ex:
        print(f"Exception: {ex}")

# Run the upload_files function directly
upload_files()
