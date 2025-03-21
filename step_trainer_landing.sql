CREATE EXTERNAL TABLE 
step_trainer_landing (
  sensorreadingtime, BIGINT,
  serialnumber, STRING,
  distancefromobject, STRING
  )
ROW FORMAT SERDE
'org.openx.data.jsonserde.JspmSerDe'
LOCATION 's3://stedi-human-balance-20250312/step_trainer_landing/
