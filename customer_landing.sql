CREATE EXTERNAL TABLE 
customer_landing_test20250321 (
  customername, STRING,
  email, STRING,
  phone, STRING,
  birthday, STRING,
  serialnumber, STRING,
  registrationdata, BIGINT,
  lastupdatedate, BIGINT,
  sharewithresearchasofdate, BIGINT,
  sharewithpublicasofdate. BIGINT,
  sharewithfriendsasofdate, BIGINT
  )
ROW FORMAT SERDE
'org.openx.data.jsonserde.JspmSerDe'
LOCATION 's3://stedi-human-balance-20250312/customer-landing/
  
