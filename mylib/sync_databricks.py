import os
import requests

from dotenv import load_dotenv

def sync_databricks_repo(branch: str = "main") -> bool:
    """
    sync GitHub repo to Databricks Repos
    
    Args:
        branch: branch name to sync, default is "main"
    
    Returns:
        bool: whether sync is successful
    """
    load_dotenv(override=True)
    host = os.environ.get('SERVER_HOSTNAME')
    token = os.environ.get('ACCESS_TOKEN')
    repository_id = os.environ.get('REPO_ID')
    
    if not all([host, token, repository_id]):
        print("Missing required credentials. Please provide SERVER_HOSTNAME, ACCESS_TOKEN, and REPO_ID")
        return False
    
    try:
        url = f"https://{host}/api/2.0/repos/{repository_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {"branch": branch}

        response = requests.patch(url, headers=headers, json=data)
        
        if response.status_code == 200:
            print(f"Successfully synced repository to branch: {branch}")
            return True
        else:
            print(f"Failed to sync repository. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error syncing repository: {str(e)}")
        return False

if __name__ == "__main__":
    # can run this script directly from command line
    sync_databricks_repo()
