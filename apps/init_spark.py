from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

def init_spark():
    password = 'br_fuel_prices'
    user = 'br_fuel_prices'
    host = '172.18.0.10'
    db_auth = ''
    database = 'br_fuel_prices'
    collection = 'fuel_prices'
    mongo_conn = f"mongodb://{user}:{password}@{host}:27017/{db_auth}"

    conf = SparkConf()

    conf.set("spark.mongodb.read.connection.uri", mongo_conn)
    conf.set("spark.mongodb.read.database", database)
    conf.set("spark.mongodb.read.collection", collection)

    conf.set("spark.mongodb.write.connection.uri", mongo_conn)
    conf.set("spark.mongodb.write.database", database)
    conf.set("spark.mongodb.write.collection", collection)
    conf.set("spark.mongodb.write.operationType", "update")

    SparkContext(conf=conf)

    return SparkSession \
        .builder \
        .appName('br_fuel_prices') \
        .getOrCreate()
