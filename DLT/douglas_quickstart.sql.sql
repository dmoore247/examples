-- Databricks notebook source
CREATE LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
AS SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`;

CREATE LIVE TABLE clickstream_prepared(
  CONSTRAINT valid_current_page EXPECT (current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
AS SELECT
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM live.clickstream_raw;

CREATE LIVE TABLE top_spark_referers
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_prepared
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10;
