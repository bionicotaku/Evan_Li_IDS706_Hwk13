import requests
import os
import io
from zipfile import ZipFile

FILESTORE_PATH = "dbfs:/FileStore/IDS_hwk13"

def is_running_on_databricks():
    """Check if the code is running on Databricks"""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ

def download_and_extract_csv(url):
    """
    Download a zip file from URL and extract the first CSV file
    Returns: CSV content if successful, None otherwise
    """
    response = requests.get(url)
    if response.status_code == 200:
        zip_content = io.BytesIO(response.content)
        with ZipFile(zip_content, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if csv_files:
                csv_file = csv_files[0]
                new_filename = "data-engineer-salary-in-2024.csv"
                print(f"Extracting CSV file: {csv_file} as {new_filename}")
                return zip_ref.read(csv_file), new_filename
            else:
                print("No CSV file found in the zip archive.")
                return None, None
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")
        return None, None

def extract(
        url="""https://www.kaggle.com/api/v1/datasets/download/chopper53/data-engineer-salary-in-2024""",
        directory=FILESTORE_PATH,
        overwrite=True
):
    """Extract a url to a file path and unzip if it's a zip file"""
    if is_running_on_databricks():
        try:
            import databricks.data
            csv_content, csv_filename = download_and_extract_csv(url)
            if csv_content is not None:
                csv_path = f"{directory}/{csv_filename}"
                dbutils.fs.put(csv_path, csv_content, overwrite)
                return csv_path
        except Exception as e:
            print(f"Error in Databricks environment: {str(e)}")
        return None
    else:
        print("Not running on Databricks")
        return None

if __name__ == "__main__":
    extract()
