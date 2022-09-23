# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT config_path DEFAULT "/dbfs/FileStore/cddp/app_a/pipeline_fruit.json";
# MAGIC CREATE WIDGET TEXT landing_path DEFAULT "/FileStore/cddp/app_a/storage/landing/";
# MAGIC CREATE WIDGET TEXT staging_path DEFAULT "/FileStore/cddp/app_a/storage/staging/";
# MAGIC CREATE WIDGET TEXT standard_path DEFAULT "/FileStore/cddp/app_a/storage/standard/";
# MAGIC CREATE WIDGET TEXT serving_path DEFAULT "/FileStore/cddp/app_a/storage/serving/";
# MAGIC CREATE WIDGET TEXT task_id DEFAULT "";

# COMMAND ----------

config_path = getArgument("config_path")
landing_path = getArgument("landing_path")
standard_path = getArgument("standard_path")
staging_path = getArgument("staging_path")
serving_path = getArgument("serving_path")
task_id = getArgument("task_id")
print(f"""config_path {config_path}""")
print(f"""landing_path {landing_path}""")
print(f"""staging_path {staging_path}""")
print(f"""standard_path {standard_path}""")
print(f"""serving_path {serving_path}""")
print(f"""task_id {task_id}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Framework Functions

# COMMAND ----------

def load_config(config_path) :
    """Loads the configuration file"""
    with open(f"{config_path}", 'r') as f:
        config = json.load(f)
    return config

def start_staging_job(spark, config, name, timeout=None):
    """Creates the staging job"""
    schema = StructType.fromJson(config["staging"][name]["schema"])
    location = config["staging"][name]["location"]
    target = config["staging"][name]["target"]
    type = config["staging"][name]["type"]
    output = config["staging"][name]["output"]
    format = config["staging"][name]["format"]
    if type == "streaming":
        df = spark \
            .readStream \
            .format(format) \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)    

        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("append")\
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt") \
                .start(staging_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":
        df = spark \
            .read \
            .format(format) \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)  

        if "table" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").save(staging_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)

    else :
        raise Exception("Invalid type")
        

def start_standard_job(spark, config, name, timeout=None):
    """Creates the standard job"""
    sql = config["standard"][name]["sql"]
    output = config["standard"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["standard"][name]["target"]

    load_staging_views()

    df = spark.sql(sql)

    type = "batch"
    if "type" in config["standard"][name]:
        type = config["standard"][name]["type"]

    if type == "streaming":

        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("append")\
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt") \
                .start(staging_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":

        if "table" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").save(standard_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
        
    else :
        raise Exception("Invalid type")


def start_serving_job(spark, config, name, timeout=None):
    """Creates the serving job"""
    sql = config["serving"][name]["sql"]
    output = config["serving"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["serving"][name]["target"]
    type = "batch"
    if "type" in config["serving"][name]:
        type = config["serving"][name]["type"]

    load_staging_views()
    load_standard_views()
    df = spark.sql(sql)

    if type == "streaming":
        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("complete")\
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("complete") \
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt") \
                .start(serving_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":
        if "table" in output:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(serving_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
    else :
        raise Exception("Invalid type")

def load_staging_views():
    if 'staging' in config:
        for name in config["staging"]:
            schema = StructType.fromJson(config["staging"][name]["schema"])
            location = config["staging"][name]["location"]
            target = config["staging"][name]["target"]
            type = config["staging"][name]["type"]
            output = config["staging"][name]["output"]
            format = config["staging"][name]["format"]
            if type == "streaming" and "view" in output:
                df = spark \
                    .readStream \
                    .format(format) \
                    .option("multiline", "true") \
                    .option("header", "true") \
                    .schema(schema) \
                    .load(landing_path+"/"+location)    
                df.createOrReplaceTempView(target)
            elif type == "batch" and "view" in output:
                df = spark \
                    .read \
                    .format(format) \
                    .option("multiline", "true") \
                    .option("header", "true") \
                    .schema(schema) \
                    .load(landing_path+"/"+location)  
                df.createOrReplaceTempView(target)


def load_standard_views():
    if 'standard' in config:
        for name in config["standard"]:
            sql = config["standard"][name]["sql"]
            output = config["standard"][name]["output"]
            if(isinstance(sql, list)):
                sql = " \n".join(sql)
            target = config["standard"][name]["target"]
            df = spark.sql(sql)
            if "view" in output:
                df.createOrReplaceTempView(target)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the pipeline

# COMMAND ----------

config = load_config(config_path)
print(f"""{config["name"]} starting""")

for name in config["staging"]:
    if not task_id or name == task_id:
        start_staging_job(spark, config, name)
for name in config["standard"]:
    if not task_id or name == task_id:
        start_standard_job(spark, config, name)
for name in config["serving"]:
    if not task_id or name == task_id:
        start_serving_job(spark, config, name)

    

# COMMAND ----------


