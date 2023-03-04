from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg
import time

S3_DATA_SOURCE_PATH = 's3://demo-mapreduce-spark/data-source/DelayedFlights-updated.csv'


def main():
    spark = SparkSession.builder.appName('Spark-Demo').getOrCreate()
    delay_flights = spark.read.csv(S3_DATA_SOURCE_PATH, header=True, inferSchema=True)
    start = time.perf_counter()
    selected_data = delay_flights.groupBy('Year').agg(avg((col('CarrierDelay') / col('ArrDelay')) * 100).alias('avg_delay'))
    end = time.perf_counter()
    selected_data.show()
    ms = (end - start)
    print(f"Elapsed {ms:.03f} secs.")

if __name__ == '__main__':
    main()
