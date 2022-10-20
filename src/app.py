from msilib import init_database
import os
import sys
import json
import csv
import tempfile
from flask import Flask, request, jsonify
import cddp
import cddp.dbxapi as dbxapi
import cddp.util as util
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from dotenv import load_dotenv

load_dotenv()
spark = cddp.create_spark_session()
app = Flask(__name__, static_url_path='/static', static_folder='../web')



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

@app.route('/api/pipeline/workflow/preview', methods=['POST'])
def preview_pipeline_workflow():
    post_data = request.get_json()
    config = post_data['pipeline']
    job_name = post_data['job_name']
    landing_path = post_data['landing_path']
    working_dir = post_data['working_dir']
    json = dbxapi.build_workflow_json(config, job_name, landing_path, working_dir)
    return jsonify({'status': 'ok', 'json': json})

# @app.route('/api/pipeline/staging/try', methods=['POST'])
# def try_pipeline_staging_task():
#     post_data = request.get_json()
#     config = post_data['pipeline']
#     if 'sample_data' in post_data:
#         sample_data = post_data['sample_data']
#     else:
#         sample_data = None
#     task_name = post_data['task']
#     timeout = post_data['timeout']
#     limit = post_data['limit']

#     with tempfile.TemporaryDirectory() as tmpdir:
#         working_dir = tmpdir+"/"+config['name']
#         config['landing_path'] = working_dir+"/landing/"
#         cddp.init(spark, config, working_dir)
#         cddp.clean_database(spark, config)
#         cddp.init_database(spark, config)

#     for task in config["staging"]:
#         if task_name == task['name'] and (task['format'] == 'csv' or task['format'] == 'json'):
#             task_landing_path = config['landing_path']+"/"+task['location']
#             if not os.path.exists(task_landing_path):
#                 os.makedirs(task_landing_path)
#             filename = sample_data[task_name]['filename']
#             content = sample_data[task_name]['content']
#             with open(task_landing_path+"/"+filename, "w") as text_file:
#                 text_file.write(content)


#     if 'staging' in config:
#         for task in config["staging"]:
#             if task_name == task['name']: 
#                 print("start staging task: "+task_name)
#                 cddp.start_staging_job(spark, config, task, timeout)
#                 json = cddp.get_dataset_as_json(spark, config, "staging", task, limit)
#                 return jsonify(json)
        
#     return jsonify({'status': 'error', 'message':'task not found'})


@app.route('/api/pipeline/standardization/try', methods=['POST'])
def try_pipeline_standardization_task():
    post_data = request.get_json()
    config = post_data['pipeline']
    task_name = post_data['task']
    timeout = post_data['timeout']
    limit = post_data['limit']

    with tempfile.TemporaryDirectory() as tmpdir:
        working_dir = tmpdir+"/"+config['name']
        config['landing_path'] = working_dir+"/landing/"
        cddp.init(spark, config, working_dir)
        cddp.clean_database(spark, config)
        cddp.init_database(spark, config)

    cddp.init_staging_sample_dataframe(spark, config)
    
    # for task in config["staging"]:
    #     if 'sampleData' in task:
    #         task_landing_path = config['landing_path']+"/"+task['name']
    #         if not os.path.exists(task_landing_path):
    #             os.makedirs(task_landing_path)
    #         filename = task['name']+".json"
    #         sampleData = task['sampleData']                
    #         with open(task_landing_path+"/"+filename, "w") as text_file:
    #             json.dump(sampleData, text_file)

    # for task in config["staging"]:
    #     if task['format'] == 'csv' or task['format'] == 'json':
    #         task_landing_path = config['landing_path']+"/"+task['location']
    #         if not os.path.exists(task_landing_path):
    #             os.makedirs(task_landing_path)
    #         sampleData = task['sampleData']     
    #         if 'sampleData' in task:
    #             sampleData = task['sampleData']                
    #             if task['format'] == 'json':
    #                 filename = task['name']+".json"
    #                 #sampleData to json
    #                 json_data = json.loads(sampleData)
    #                 with open(task_landing_path+"/"+filename, "w") as text_file:
    #                      json.dump(sampleData, text_file)
    #             elif task['format'] == 'csv':
    #                 filename = task['name']+".csv"
    #                 #sampleData to csv format
    #                 util.json2csv(sampleData, task_landing_path+"/"+filename)

    # if 'staging' in config:
    #     for task in config["staging"]:
    #         print("start staging task: "+task['name'])
    #         cddp.start_staging_job(spark, config, task, timeout)
    
    if 'standard' in config:
        for task in config["standard"]:
            if task_name == task['name']: 
                print("start standardization task: "+task_name)
                cddp.start_standard_job(spark, config, task, False, True, timeout)
                result = cddp.get_dataset_as_json(spark, config, "standard", task, limit)
                data_str = json.dumps(result)
                return jsonify({"data": data_str})
        
    return jsonify({'status': 'error', 'message':'task not found'})

@app.route('/api/pipeline/serving/try', methods=['POST'])
def try_pipeline_serving_task():
    post_data = request.get_json()
    config = post_data['pipeline']
    if 'sample_data' in post_data:
        sample_data = post_data['sample_data']
    else:
        sample_data = None
    task_name = post_data['task']
    timeout = post_data['timeout']
    limit = post_data['limit']

    with tempfile.TemporaryDirectory() as tmpdir:
        working_dir = tmpdir+"/"+config['name']
        config['landing_path'] = working_dir+"/landing/"
        cddp.init(spark, config, working_dir)
        cddp.clean_database(spark, config)
        cddp.init_database(spark, config)
    
    cddp.init_staging_sample_dataframe(spark, config)
    
    # for task in config["staging"]:
    #     if 'sampleData' in task:
    #         task_landing_path = config['landing_path']+"/"+task['name']
    #         if not os.path.exists(task_landing_path):
    #             os.makedirs(task_landing_path)
    #         filename = task['name']+".json"
    #         sampleData = task['sampleData']                
    #         with open(task_landing_path+"/"+filename, "w") as text_file:
    #             json.dump(sampleData, text_file)
    #         schema = StructType.fromJson(task["schema"])
    #         df = spark \
    #             .read \
    #             .format(format) \
    #             .option("multiline", "true") \
    #             .option("header", "true") \
    #             .schema(schema) \
    #             .load(task_landing_path+"/"+filename)  

    #         if "table" in output:
    #             df.write.format(storage_format).mode("append").option("overwriteSchema", "true").saveAsTable(target)
    #         if "file" in output:
    #             df.write.format(storage_format).mode("append").option("overwriteSchema", "true").save(staging_path+"/"+target)
    #         if "view" in output:
    #             df.createOrReplaceTempView(target)

    # for task in config["staging"]:
    #     if task['format'] == 'csv' or task['format'] == 'json':
    #         task_landing_path = config['landing_path']+"/"+task['location']
    #         if not os.path.exists(task_landing_path):
    #             os.makedirs(task_landing_path)
    #         if 'sampleData' in task:
    #             sampleData = task['sampleData']                
    #             if task['format'] == 'json':
    #                 filename = task['name']+".json"
    #                 #sampleData to json
    #                 json_data = json.loads(sampleData)
    #                 with open(task_landing_path+"/"+filename, "w") as text_file:
    #                      json.dump(sampleData, text_file)
    #             elif task['format'] == 'csv':
    #                 filename = task['name']+".csv"
    #                 #sampleData to csv format
    #                 util.json2csv(sampleData, task_landing_path+"/"+filename)

    # if 'staging' in config:
    #     for task in config["staging"]:
    #         print("start staging task: "+task['name'])
    #         cddp.start_staging_job(spark, config, task, timeout)
    
    if 'standard' in config:
        for task in config["standard"]:
                print("start standardization task: "+task['name'])
                cddp.start_standard_job(spark, config, task, False, True, timeout)

    if 'serving' in config:
        for task in config["serving"]:
            if task_name == task['name']: 
                print("start serving task: "+task['name'])
                cddp.start_serving_job(spark, config, task, False, True, timeout)
                result = cddp.get_dataset_as_json(spark, config, "serving", task, limit)
                data_str = json.dumps(result)
                return jsonify({"data": data_str})

    return jsonify({'status': 'error', 'message':'task not found'})


@app.route('/api/pipeline/staging/load_sample_data', methods=['POST'])
def load_sample_data():
    post_data = request.get_json()
    if 'sample_data' in post_data:
        sample_data = post_data['sample_data']
        sample_data_format = post_data['sample_data_format']
        json, schema = cddp.load_sample_data(spark, sample_data, sample_data_format)
        response = {'status': 'success', 'data':json, 'schema':schema}
        return jsonify(response)        
    else:
        return jsonify({'status': 'error', 'message':'data not found'})