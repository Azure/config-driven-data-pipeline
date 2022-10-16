import json
import csv

def json2csv(jsondata, output_path): 
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





