import requests
import os
from dotenv import load_dotenv
import time
import base64


load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
job_id = os.getenv("JOB_ID")
server_h = os.getenv("SERVER_HOSTNAME")

url = f"https://{server_h}/api/2.0/jobs/run-now"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

data = {"job_id": job_id}

# define function to check job status
def check_job_status(run_id):
    status_url = f"https://{server_h}/api/2.0/jobs/runs/get"
    params = {"run_id": run_id}
    total_time = 0

    while True:
        status_response = requests.get(status_url, headers=headers, params=params)
        if status_response.status_code == 200:
            status_data = status_response.json()
            life_cycle_state = status_data.get("state").get("life_cycle_state")

            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = status_data.get("state").get("result_state")
                print(f"Job completed with status: {result_state}")
                return result_state == "SUCCESS"
            else:
                print(
                    f"Job still running... Current state: {life_cycle_state}, total time: {total_time} seconds"
                )
                time.sleep(15)  # check every 15 seconds
                total_time += 15
        else:
            print(f"Error checking status: {status_response.status_code}")
            return False


def download_md():
    """Download the markdown file from Databricks FileStore"""
    file_path = "/FileStore/IDS_hwk13/analysis_results.md"
    base_url = f"https://{server_h}/api/2.0/dbfs/read"

    try:
        response = requests.get(
            base_url,
            headers=headers,
            params={
                "path": file_path,
                "length": 2
                ** 20,  # request a large enough buffer to ensure we get the entire file
            },
        )

        response.raise_for_status()  # if status code is not 200, raise an exception
        content = response.json()

        if "data" not in content:
            return "Error: No data in response"

        # decode and save the file
        decoded_content = base64.b64decode(content["data"]).decode("utf-8")
        with open("analysis_results.md", "w", encoding="utf-8") as f:
            f.write(decoded_content)

        return "File successfully downloaded and saved locally"

    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {str(e)}"


def main():
    """Trigger the job and check its status"""
    response = requests.post(url, headers=headers, json=data)
    # get run id
    run_id = response.json().get("run_id")
    # if job run successfully triggered, check its status and download the analysis results
    if response.status_code == 200:
        print("Job run successfully triggered")
        job_success = check_job_status(run_id)
        print(f"Job finished successfully: {job_success}")
        print(download_md())
    else:
        print(f"Error: {response.status_code}, {response.text}")


if __name__ == "__main__":
    main()
