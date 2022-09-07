from local_runner import create_spark_session, load_config, show_serving_dataset

if __name__ == "__main__":

    config = load_config()
    print("app name: "+config["name"])
    spark = create_spark_session(config)
    for name in config["serving"]:
        show_serving_dataset(spark, config, name)