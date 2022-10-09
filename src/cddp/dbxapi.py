from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
import json
import os
import tempfile
from dotenv import load_dotenv
load_dotenv()

def deploy_pipeline(config, job_name, landing_path, working_dir, run_now=False):
    
    api_client = ApiClient(
        host  = os.getenv('DATABRICKS_HOST'),
        token = os.getenv('DATABRICKS_TOKEN')
    )
    jobs_api = JobsApi(api_client)
    app_name = config["name"]
    remote_landing_path = DbfsPath.from_api_path(landing_path)
    remote_working_dir = DbfsPath.from_api_path(working_dir)
    # upload config to dbfs
    dbfs_api = DbfsApi(api_client)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_config_path = f"{tmpdir}/{app_name}.json"
        with open(tmp_config_path, 'w') as outfile:
            json.dump(config, outfile)
        remote_config_folder_path = f"/FileStore/cddp_apps/configs/{app_name}"
        remote_config_folder = DbfsPath.from_api_path(remote_config_folder_path)
        remote_config_path = f"{remote_config_folder.absolute_path}/pipeline.json"
        if dbfs_api.file_exists(remote_config_folder):
            dbfs_api.mkdirs(remote_config_folder)
        if dbfs_api.file_exists(remote_landing_path):
            dbfs_api.mkdirs(remote_landing_path)
        if dbfs_api.file_exists(remote_working_dir):
            dbfs_api.mkdirs(remote_working_dir)
        dbfs_api.put_file(tmp_config_path, DbfsPath(remote_config_path), True)


    body = build_workflow_json(config, job_name, remote_landing_path.absolute_path, remote_working_dir.absolute_path)
    response = jobs_api.create_job(json = body)
    job_id = response["job_id"]
    if run_now:
        jobs_api.run_now(job_id = job_id, jar_params = None, notebook_params = None, python_params = None, spark_submit_params = None)
    return response

def build_workflow_json(config, job_name, landing_path, working_dir):
    app_name = config["name"]
    remote_config_folder_path = f"/FileStore/cddp_apps/configs/{app_name}"
    config_path = f"/dbfs{remote_config_folder_path}/pipeline.json"
    dbx_cluster = os.getenv("DATABRICKS_CLUSTER")
    tasks = build_tasks(config, landing_path, working_dir, config_path, dbx_cluster)
    body = {
            "name": job_name,
            "max_concurrent_runs": 1,
            "tasks": tasks
        }
    return body

def build_tasks(config, landing_path, working_dir, config_path, dbx_cluster):
    tasks = []
    standard_gate = create_stage_gate_task("standard", dbx_cluster)
    serving_gate = create_stage_gate_task("serving", dbx_cluster)
    serving_gate["depends_on"].append({"task_key": standard_gate["task_key"]})
    tasks.append(standard_gate)
    tasks.append(serving_gate)
    for task in config["staging"]:
        type = task["type"]
        name = task['name']
        output = task["output"]
        if 'table' in output or 'file' in output:
            task_obj = create_task("staging", name, landing_path, working_dir, config_path, dbx_cluster)            
            if type == "batch":
                standard_gate["depends_on"].append({"task_key": name})
            tasks.append(task_obj)

    for task in config["standard"]:
        type = task["type"]
        name = task['name']
        output = task["output"]
        if 'table' in output or 'file' in output:
            task_obj = create_task("standard", name, landing_path, working_dir, config_path, dbx_cluster)            
            if type == "batch":
                serving_gate["depends_on"].append({"task_key": name})
            task_obj["depends_on"].append({"task_key": standard_gate["task_key"]})
            tasks.append(task_obj)
    
    for task in config["serving"]:
        type = task["type"]
        name = task['name']
        task_obj = create_task("serving", name, landing_path, working_dir, config_path, dbx_cluster)            
        task_obj["depends_on"].append({"task_key": serving_gate["task_key"]})
        tasks.append(task_obj)

    return tasks

def create_task(stage, name, landing_path, working_dir, config_path, dbx_cluster):
    return {
        "task_key": name,
        "description": name,
        "depends_on": [],
        "python_wheel_task": {
            "package_name": "cddp",
            "entry_point": "entrypoint",
            "named_parameters": {
                "stage": stage,
                "task": name,
                "landing-path": landing_path,
                "working-dir": working_dir,
                "config-path": config_path
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

def create_stage_gate_task(stage, dbx_cluster):
    return {
        "task_key": f"{stage}_gate",
        "depends_on": [],
        "python_wheel_task": {
        "package_name": "cddp",
        "entry_point": "wait_for_next_stage",
        "named_parameters": {
                "duration": "5"
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
        "timeout_seconds": 0,
        "email_notifications": {}
    }
