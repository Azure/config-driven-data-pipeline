#!/usr/bin/env python3
import json
import os
import sys
from dotenv import load_dotenv
import requests
load_dotenv()
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

api_client = ApiClient(
  host  = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)
jobs_api = JobsApi(api_client)

dbx_cluster = os.getenv("DATABRICKS_CLUSTER")

job_name = "cddp_test_job"
app_name = "cddp_test_app"

body = {
        "name": job_name,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": app_name,
                "description": app_name,
                "python_wheel_task": {
                    "package_name": "cddp",
                    "entry_point": "entrypoint",
                    "named_parameters": {
                        "stage": "staging",
                        "task": "sales",
                        "landing-path": f"/FileStore/cddp_apps/{app_name}/landing/",
                        "working-dir": f"/FileStore/cddp_apps/{app_name}/",
                        "config-path": f"/dbfs/FileStore/cddp_apps/{app_name}/pipeline.json"
                    }
                },
                "existing_cluster_id": dbx_cluster,
                "libraries": [
                    {
                        "pypi": {
                            "package": "cddp"
                        }
                    }
                ],
            }
        ]
    }
response = jobs_api.create_job(json = body)
print(response)

# response = requests.post(api_url, json = body, 
#     headers = {'Authorization': f'Bearer {dbx_token}'})
# if response.status_code == 200:
#     print(response.json())
# else:
#     print(response.text)


