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

def isRunningOnDatabricks():
    return os.getenv("SPARK_HOME") == "/databricks/spark"


if __name__ == "__main__":
    jsondata = [{"id": 1, "name": "John", "age": 30}, {"id": 2, "name": "Peter", "age": 25}, {"id": 3, "name": "Mary", "age": 28}]
    output_path = "data.csv"
    json_to_csv(jsondata, output_path)
