from pyspark.sql import SparkSession

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "mysql-connector-java-8.0.28.jar,snowflake-jdbc-3.13.10.jar,spark-snowflake_2.12-2.9.2-spark_3.1.jar") \
        .getOrCreate()

def read_mysql_table(spark, config, table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", config['url']) \
        .option("driver", config['driver']) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

def write_to_snowflake(df, config, table_name):
    df.write \
        .format("snowflake") \
        .options(**config) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()