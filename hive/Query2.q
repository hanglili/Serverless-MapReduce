CREATE EXTERNAL TABLE IF NOT EXISTS uservisits (
  sourceIP STRING,
  destURL STRING,
  visitDate STRING,
  adRevenue DOUBLE,
  userAgent STRING,
  countryCode STRING,
  languageCode STRING,
  searchWord STRING,
  duration INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}/uservisits';

-- Query
INSERT OVERWRITE DIRECTORY '${OUTPUT}/query2/' SELECT SUBSTR(sourceIP, 1, 7), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 7);