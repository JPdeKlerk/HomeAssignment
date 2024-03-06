import json
import os
import requests

def fetch_all_repositories(org_name):
    # GitHub API endpoint to fetch repositories of an organization
    url = f"https://api.github.com/orgs/{org_name}/repos"
    
    # Send GET request to fetch repositories
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        repositories = response.json()
        return repositories
    else:
        print(f"Failed to fetch repositories. Status code: {response.status_code}")
        return None

def fetch_pull_requests(repo_owner, repo_name):
    # GitHub API endpoint to fetch pull requests of a repository
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/pulls"
    
    # Send GET request to fetch pull requests
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        pull_requests = response.json()
        return pull_requests
    else:
        print(f"Failed to fetch pull requests for {repo_owner}/{repo_name}. Status code: {response.status_code}")
        return None

def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file)

# Fetch repositories from the Scytale-exercise organization
org_name = "Scytale-exercise"
repositories = fetch_all_repositories(org_name)

if repositories:
    # Create a directory to save JSON files if it doesn't exist
    if not os.path.exists("json_files"):
        os.makedirs("json_files")
    
    for repo in repositories:
        # Fetch pull requests for each repository
        repo_owner = repo["owner"]["login"]
        repo_name = repo["name"]
        pull_requests = fetch_pull_requests(repo_owner, repo_name)
        
        if pull_requests:
            # Save pull requests data to a JSON file
            file_path = f"Files/json_files/{repo_name}_pull_requests.json"
            save_json(pull_requests, file_path)
            
            print(f"Pull requests for {repo_owner}/{repo_name} saved to {file_path}")
        else:
            print(f"No pull requests found for {repo_owner}/{repo_name}")

else:
    print("Failed to fetch repositories from the organization.")
