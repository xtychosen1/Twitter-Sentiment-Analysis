

##Two Tables From FAN XINZE

CREATE EXTERNAL TABLE IF NOT EXISTS `elonmusk`.`pred` (
  `sentiment` string,
  `tweet` string,
  `label` int,
  `prediction` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-xinzhen/project/predictions/';



CREATE EXTERNAL TABLE IF NOT EXISTS `elonmusk`.`d3` (
  `id` string,
  `user_name` string,
  `screen_name` string,
  `text` string,
  `follower_count` int,
  `location` string,
  `geo` string,
  `created_at` string,
  `sentiment` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-xinzhen/project/d3/';



##Two Tables From XU TIANYUAN
## create first table from CSV file in bucket
CREATE EXTERNAL TABLE IF NOT EXISTS `twitter`.`twitter` (
  `id` string,
  `user_name` string,
  `screen_name` string,
  `text` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string,
  `sentiment` string,
  `timestamp` timestamp,
  `day_of_week` string,
  `date` date,
  `time` string,
  `word_count` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-xuty/twitter/EDA.csv/';

## create second table to add new column for quicksight
CREATE TABLE new_table_name AS
SELECT
    *,
    CASE
        WHEN location LIKE '%California%' OR location LIKE '%CA%' OR location LIKE '%Los Angeles%' OR location LIKE '%San Francisco%' THEN 'California'
        WHEN location LIKE '%Texas%' OR location LIKE '%TX%' OR location LIKE '%Houston%' OR location LIKE '%Dallas%' THEN 'Texas'
        WHEN location LIKE '%Florida%' OR location LIKE '%FL%' OR location LIKE '%Miami%' OR location LIKE '%Orlando%' THEN 'Florida'
        WHEN location LIKE '%New York%' THEN 'New York'
        WHEN location = 'United States' THEN 'USA'
        ELSE location
    END as new_location,
    CASE
        WHEN followers_count BETWEEN 0 AND 1000 THEN '0 - 1,000'
        WHEN followers_count BETWEEN 1001 AND 10000 THEN '1,001 - 10,000'
        WHEN followers_count BETWEEN 10001 AND 100000 THEN '10,001 - 100,000'
        WHEN followers_count BETWEEN 100001 AND 1000000 THEN '100,001 - 1,000,000'
        ELSE '1,000,001+'
    END as followers_range
FROM
    twitter;


