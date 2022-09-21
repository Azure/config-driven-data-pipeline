from local_main import create_spark_session, load_config, show_serving_dataset
from sys import argv

if __name__ == "__main__":
    pipeline_path = argv[1];
    config = load_config(pipeline_path)
    print("app name: "+config["name"])
    spark = create_spark_session(config)
    for name in config["serving"]:
        show_serving_dataset(spark, config, name)