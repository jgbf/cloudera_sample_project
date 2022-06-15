import os
from pyspark.sql.functions import desc
from pyspark.sql.functions import col

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def read_files(folder: str):
    sc = SparkContext('local')
    spark = SparkSession(sc)
    return spark.read.option('header', 'true').csv(folder)

def main():
    data = read_files('/nifi/decompressed')

    # Get the most departued airport
    top_dep = data.groupby('Origin').count().sort(desc('count'))
    top_dep.coalesce(1).write.option('header', 'true').csv('/nifi/top_departured_airports.csv')

    # Get total number of fligths per carrier
    carrier_flight = data.groupby('UniqueCarrier').count().sort(desc('count'))
    carrier_flight.coalesce(1).write.option('header', 'true').csv('/nifi/carrier_count.csv')

    # Calculate the most dealyed carriers
    data = data.withColumn('delay', col('CarrierDelay').cast('int'))
    delays = data.groupby('UniqueCarrier').sum('delay').sort(desc('sum(delay)'))
    delays.coalesce(1).write.option('header', 'true').csv('/nifi/delays.csv')


if __name__ == '__main__':
    main()
