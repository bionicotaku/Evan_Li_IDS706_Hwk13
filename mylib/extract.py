import requests
from dotenv import load_dotenv
import os
import json
import base64
import io
from zipfile import ZipFile

# Load environment variables
load_dotenv(override=True)
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/IDS_hwk13"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, 
                           data=json.dumps(data), 
                           verify=True, 
                           headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data['path'] = path
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)
  

def create(path, overwrite, headers):
    _data = {}
    _data['path'] = path
    _data['overwrite'] = overwrite
    response = perform_query('/dbfs/create', headers=headers, data=_data)
    print(f"Create API response: {response}")
    return response


def add_block(handle, data, headers):
    _data = {}
    _data['handle'] = handle
    _data['data'] = data
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data['handle'] = handle
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file_from_url(content, dbfs_path, overwrite, headers):
    try:
        create_response = create(dbfs_path, overwrite, headers=headers)
        handle = create_response['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, 
                        base64.standard_b64encode(content[i:i+2**20]).decode(), 
                        headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    except KeyError as e:
        print(f"Error: Failed to get handle from create response. Response: {create_response}")
        return False
    except Exception as e:
        print(f"Error during file upload: {str(e)}")
        return False
    return True


def download_and_extract_csv(url):
    """
    Download a zip file from URL and extract the first CSV file
    Returns: CSV content if successful, None otherwise
    """
    response = requests.get(url)
    if response.status_code == 200:
        # Create a ZipFile object from the downloaded content
        zip_content = io.BytesIO(response.content)
        with ZipFile(zip_content, 'r') as zip_ref:
            # Get the first CSV file in the zip
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
    # Make the directory
    mkdirs(path=directory, headers=headers)
    print(f"Directory {directory} created successfully.")
    # Download and extract the CSV
    csv_content, csv_filename = download_and_extract_csv(url)
    if csv_content is not None:
        # Upload the CSV file directly using put_file_from_url
        csv_path = f"{directory}/{csv_filename}"
        if put_file_from_url(csv_content, csv_path, overwrite, headers):
            return csv_path
    return None


if __name__ == "__main__":
    extract()
