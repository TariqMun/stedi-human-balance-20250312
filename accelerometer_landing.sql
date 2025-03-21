CREATE EXTERNAL TABLE 
accelerometer_landing (
  user, STRING,
  timestamp, BIGINT,
  x, FLOAT,
  y, FLOAT,
  z, FLOAT
  )
ROW FORMAT SERDE
'org.openx.data.jsonserde.JspmSerDe'
LOCATION 's3://stedi-human-balance-20250312/accelerometer-landing/
