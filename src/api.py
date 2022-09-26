import os
import tempfile
from flask import Flask, request, jsonify
from main import create_spark_session, start_staging_job, start_standard_job, start_serving_job, get_dataset_as_json, init
spark = create_spark_session()
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
        init(spark, config, working_dir)

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
            start_staging_job(spark, config, name, timeout)
    if 'standard' in config:
        for name in config["standard"]:
            print("start standardization task: "+name)
            start_standard_job(spark, config, name, timeout)
    if 'serving' in config:
        for name in config["serving"]:
            print("start serving task: "+name)
            start_serving_job(spark, config, name, timeout)
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
    json = get_dataset_as_json(spark, config, stage_name, task_name, limit)
    return jsonify(json)