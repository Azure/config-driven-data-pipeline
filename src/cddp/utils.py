import os
import json
import csv

def json_to_csv(jsondata, output_path): 
    data_file = open(output_path, 'w', newline='')
    csv_writer = csv.writer(data_file)
    count = 0
    for data in jsondata:
        if count == 0:
            header = data.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(data.values())
    
    data_file.close()

def is_running_on_databricks():
    return os.getenv("SPARK_HOME") == "/databricks/spark"


def get_path_for_current_env(input_type, path):
    # Remove the '/' in the path to ensure the sucessful CI pipeline run
    if input_type=="filestore": 
        if path.startswith("/FileStore") and not is_running_on_databricks():
            path=path[1:]
    return path

if __name__ == "__main__":
    jsondata = [{"id": 1, "name": "John", "age": 30}, {"id": 2, "name": "Peter", "age": 25}, {"id": 3, "name": "Mary", "age": 28}]
    output_path = "data.csv"
    json_to_csv(jsondata, output_path)
