from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

def convert_date(date_string):
    if date_string is None:
        return None
    
    if re.match(r'\d{4}-\d{2}-\d{2}', date_string):
        return date_string
    
    mois_to_num = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }
    
    match = re.search(r'(\d{2}) (\w+) (\d{4})', date_string)
    if match:
        jour, mois, annee = match.groups()
        mois_num = mois_to_num.get(mois.lower())
        if mois_num:
            return f"{annee}-{mois_num}-{jour}"
    
    return None

def extract_transform_load():
    spark = SparkSession.builder \
        .appName("ToysAndModelsETL") \
        .config("spark.jars", "mysql-connector-java-8.0.28.jar,snowflake-jdbc-3.13.10.jar,spark-snowflake_2.12-2.9.2-spark_3.1.jar") \
        .getOrCreate()

    # Lire la configuration
    with open('config/mysql_config.json', 'r') as f:
        mysql_config = json.load(f)
    
    with open('config/snowflake_config.json', 'r') as f:
        snowflake_config = json.load(f)

    # Extraction depuis MySQL
    df_orders = spark.read \
        .format("jdbc") \
        .option("url", mysql_config['url']) \
        .option("driver", mysql_config['driver']) \
        .option("dbtable", "orders") \
        .option("user", mysql_config['user']) \
        .option("password", mysql_config['password']) \
        .load()

    # Conversion des dates
    convert_date_udf = udf(convert_date, StringType())
    df_orders_corrected = df_orders.withColumn(
        "orderDate",
        to_date(convert_date_udf(col("orderDate")))
    )

    # Chargement dans Snowflake
    df_orders_corrected.write \
        .format("snowflake") \
        .options(**snowflake_config) \
        .option("dbtable", "ORDERS_CLEANED") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    extract_transform_load()