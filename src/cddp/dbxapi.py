from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
import json
import os
import tempfile
import cddp.utils as utils
from dotenv import load_dotenv
load_dotenv()

def deploy_pipeline(config, job_name, working_dir, run_now=False):
    
    api_client = ApiClient(
        host  = os.getenv('DATABRICKS_HOST'),
        token = os.getenv('DATABRICKS_TOKEN')
    )
    jobs_api = JobsApi(api_client)
    app_name = config["name"]
    remote_working_dir = DbfsPath.from_api_path(working_dir)
    # upload config to dbfs
    dbfs_api = DbfsApi(api_client)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_config_path = f"{tmpdir}/{app_name}.json"
        with open(tmp_config_path, 'w') as outfile:
            json.dump(config, outfile)
        remote_config_folder_path = f"/FileStore/cddp_apps/{app_name}"
        remote_config_folder = DbfsPath.from_api_path(remote_config_folder_path)
        remote_config_path = f"{remote_config_folder.absolute_path}/pipeline.json"
        remote_landing_path = f"{remote_config_folder.absolute_path}/landing/"
        remote_landing_folder = DbfsPath(remote_landing_path)
        if not dbfs_api.file_exists(remote_config_folder):
            dbfs_api.mkdirs(remote_config_folder)
        if not dbfs_api.file_exists(remote_landing_folder):
            dbfs_api.mkdirs(remote_landing_folder)
        if not dbfs_api.file_exists(remote_working_dir):
            dbfs_api.mkdirs(remote_working_dir)
        dbfs_api.put_file(tmp_config_path, DbfsPath(remote_config_path), True)

        #upload sample data
        for task in config["staging"]:
            type = task["input"]["type"]            
            if type == "filestore":
                name = task["name"]
                format = task["input"]["format"]
                remote_landing_data_folder = DbfsPath(remote_landing_path+"/"+name)
                if not dbfs_api.file_exists(remote_landing_data_folder):
                    dbfs_api.mkdirs(remote_landing_data_folder)
                sample_data = task["sampleData"]
                if not os.path.exists(f"{tmpdir}/data/"):
                        os.mkdir(f"{tmpdir}/data/")
                if format == "csv":
                    tmp_data_path = f"{tmpdir}/data/{name}.csv"
                    utils.json_to_csv(sample_data, tmp_data_path)
                    dbfs_api.put_file(tmp_data_path, DbfsPath(remote_landing_path+"/"+name+"/data.csv"), True)
                elif format == "json":
                    tmp_data_path = f"{tmpdir}/data/{name}.json"
                    with open(tmp_data_path, 'w') as sample_data_outfile:
                        json.dump(sample_data, sample_data_outfile)
                    dbfs_api.put_file(tmp_data_path, DbfsPath(remote_landing_path+"/"+name+"/data.json"), True)
       
    body = build_workflow_json(config, job_name, remote_working_dir.absolute_path)
    response = jobs_api.create_job(json = body)
    job_id = response["job_id"]
    if run_now:
        jobs_api.run_now(job_id = job_id, jar_params = None, notebook_params = None, python_params = None, spark_submit_params = None)
    return response

def build_workflow_json(config, job_name, working_dir):
    app_name = config["name"]
    remote_config_folder_path = f"/FileStore/cddp_apps/{app_name}"
    config_path = f"/dbfs{remote_config_folder_path}/pipeline.json"
    dbx_cluster = os.getenv("DATABRICKS_CLUSTER")
    tasks = build_tasks(config, working_dir, config_path, dbx_cluster)
    body = {
            "name": job_name,
            "max_concurrent_runs": 1,
            "tasks": tasks
        }
    return body

def build_tasks(config, working_dir, config_path, dbx_cluster):
    tasks = []
    standard_gate = create_stage_gate_task("standard", dbx_cluster)
    serving_gate = create_stage_gate_task("serving", dbx_cluster)
    serving_gate["depends_on"].append({"task_key": standard_gate["task_key"]})
    tasks.append(standard_gate)
    tasks.append(serving_gate)
    for task in config["staging"]:
        type = task["input"]["type"]
        if type == "filestore":
            mode = task["input"]["read-type"]
        elif type == "azure_adls_gen2":
            mode = task["input"]["read-type"]
        name = task["name"]
        output = task["output"]["type"]
        if 'table' in output or 'file' in output:
            task_obj = create_task("staging", name, working_dir, config_path, dbx_cluster)            
            if mode == "batch":
                standard_gate["depends_on"].append({"task_key": name})
            tasks.append(task_obj)

    for task in config["standard"]:
        type = task["type"]
        name = task['name']
        output_type = task["output"]["type"]
        dependency = task["dependency"]
        if 'table' in output_type or 'file' in output_type:
            task_obj = create_task("standard", name, working_dir, config_path, dbx_cluster)            
            if type == "batch":
                serving_gate["depends_on"].append({"task_key": name})
            task_obj["depends_on"].append({"task_key": standard_gate["task_key"]})
            tasks.append(task_obj)
            for dep in dependency:
                if(config["standard"][dep]["type"] == "batch"):
                    task_obj["depends_on"].append({"task_key": dep})
    
    for task in config["serving"]:
        type = task["type"]
        name = task['name']
        dependency = task["dependency"]
        task_obj = create_task("serving", name, working_dir, config_path, dbx_cluster)            
        task_obj["depends_on"].append({"task_key": serving_gate["task_key"]})
        tasks.append(task_obj)
        for dep in dependency:
            if(config["serving"][dep]["type"] == "batch"):
                task_obj["depends_on"].append({"task_key": dep})

    return tasks

def create_task(stage, name, working_dir, config_path, dbx_cluster):
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
