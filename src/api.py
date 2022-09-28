from msilib import init_database
import os
import sys
import json
import tempfile
from flask import Flask, request, jsonify
import cddp
import cddp.dbxapi as dbxapi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from dotenv import load_dotenv
load_dotenv()
spark = cddp.create_spark_session()
app = Flask(__name__, static_url_path='/static', static_folder='../web')


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/api/pipeline/preview', methods=['POST'])
def preview_pipeline():
    post_data = request.get_json()
    config = post_data['pipeline']
    sampleData = post_data['sampleData']
    timeout = post_data['timeout']

    with tempfile.TemporaryDirectory() as tmpdir:
        working_dir = tmpdir+"/"+config['name']
        config['landing_path'] = working_dir+"/landing/"
        cddp.init(spark, config, working_dir)
        cddp.clean_database(spark, config)
        cddp.init_database(spark, config)

    for name in config["staging"]:
        if(config["staging"][name]['format'] == 'csv' or config["staging"][name]['format'] == 'json'):
            task_landing_path = config['landing_path']+"/"+config["staging"][name]['location']
            if not os.path.exists(task_landing_path):
                os.makedirs(task_landing_path)
            filename = sampleData[name]['filename']
            content = sampleData[name]['content']
            with open(task_landing_path+"/"+filename, "w") as text_file:
                text_file.write(content)


    if 'staging' in config:
        for name in config["staging"]:
            print("start staging task: "+name)
            cddp.start_staging_job(spark, config, name, timeout)
    if 'standard' in config:
        for name in config["standard"]:
            print("start standardization task: "+name)
            cddp.start_standard_job(spark, config, name, timeout)
    if 'serving' in config:
        for name in config["serving"]:
            print("start serving task: "+name)
            cddp.start_serving_job(spark, config, name, timeout)
    return jsonify({'status': 'ok', 'working_dir': config['working_dir']})

@app.route('/api/pipeline/result', methods=['POST'])
def show_pipeline_task_result():
    post_data = request.get_json()
    config = post_data['pipeline']
    working_dir = post_data['working_dir']
    config['working_dir'] = working_dir
    stage_name = post_data['stage']
    task_name = post_data['task']
    limit = post_data['limit']
    print("app name: "+config["name"])
    json = cddp.get_dataset_as_json(spark, config, stage_name, task_name, limit)
    return jsonify(json)

@app.route('/api/pipeline/deploy', methods=['POST'])
def deploy_pipeline():
    post_data = request.get_json()
    config = post_data['pipeline']
    job_name = post_data['job_name']
    landing_path = post_data['landing_path']
    working_dir = post_data['working_dir']
    row_now = post_data['row_now']
    dbxapi.deploy_pipeline(config, job_name, landing_path, working_dir, row_now)
    return jsonify({'status': 'ok'})
