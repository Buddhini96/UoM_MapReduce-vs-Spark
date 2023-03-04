DROP TABLE IF EXISTS delay_flights;

CREATE EXTERNAL TABLE delay_flights (
ID INT,
Year INT,
Month INT,
DayofMonth INT,
DayOfWeek INT,
DepTime INT,
CRSDepTime INT,
ArrTime INT,
CRSArrTime INT,
UniqueCarrier STRING,
FlightNum INT,
TailNum STRING,
ActualElapsedTime INT,
CRSElapsedTime INT,
AirTime INT,
ArrDelay INT,
DepDelay INT,
Origin STRING,
Dest STRING,
Distance INT,
TaxiIn INT,
TaxiOut INT,
Cancelled INT,
CancellationCode STRING,
Diverted INT,
CarrierDelay INT,
WeatherDelay INT,
NASDelay INT,
SecurityDelay INT,
LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION "${INPUT}"
TBLPROPERTIES ("skip.header.line.count"="1");

INSERT OVERWRITE DIRECTORY "${OUTPUT}"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS delay_percentage
FROM delay_flights
GROUP BY Year;