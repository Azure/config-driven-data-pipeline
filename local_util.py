from local_main import create_spark_session, load_config, get_dataset_as_json
from sys import argv

if __name__ == "__main__":
    pipeline_path = argv[1];
    config = load_config(pipeline_path)
    print("app name: "+config["name"])
    spark = create_spark_session()


    for name in config["serving"]:
        json = get_dataset_as_json(spark, config, "serving", name)
        print(json)