# UoM_MapReduce-vs-Spark
Comparing Hadoop MapReduce and Spark Frameworks

Measuring the time taken to execute below query in data set delayedFlights-updated.csv Hadoop MapReduce framework and Spark framework. 

result = sqlContext.sql("SELECT Year, avg((CarrierDelay /ArrDelay)*100) from delay_flights GROUP BY Year").show()
