import requests
import os
from dotenv import load_dotenv
import time

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

response = requests.post(url, headers=headers, json=data)

# 获取运行ID
run_id = response.json().get("run_id")

# 定义检查作业状态的函数
def check_job_status(run_id):
    status_url = f"https://{server_h}/api/2.0/jobs/runs/get"
    params = {"run_id": run_id}

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
                print(f"Job still running... Current state: {life_cycle_state}")
                time.sleep(30)  # 每30秒检查一次
        else:
            print(f"Error checking status: {status_response.status_code}")
            return False


def download_md():
    response = requests.get(
        f"https://{server_h}/api/2.0/dbfs/get-status?path=dbfs:/FileStore/IDS_hwk13/analysis_results.md"
    )
    return response.content


# 如果作业触发成功，检查其状态
if response.status_code == 200:
    print("Job run successfully triggered")
    job_success = check_job_status(run_id)
    print(f"Job finished successfully: {job_success}")
    print(download_md())
else:
    print(f"Error: {response.status_code}, {response.text}")
