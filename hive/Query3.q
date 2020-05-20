CREATE EXTERNAL TABLE IF NOT EXISTS rankings (
  pageURL STRING,
  pageRank INT,
  avgDuration INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}/rankings';

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
INSERT OVERWRITE DIRECTORY '${OUTPUT}/query3/'
SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank
FROM rankings R JOIN
    (SELECT sourceIP, destURL, adRevenue
     FROM uservisits UV
     WHERE UV.visitDate < "2000-01-01")
NUV ON (R.pageURL = NUV.destURL)
GROUP BY sourceIP
ORDER BY totalRevenue DESC
LIMIT 1;