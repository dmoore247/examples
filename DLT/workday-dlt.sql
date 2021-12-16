-- Databricks notebook source
CREATE LIVE VIEW workday_employee_bronze
COMMENT "The workday employee main report from Workday"
AS SELECT * 
FROM workday_douglas_moore.workday

-- COMMAND ----------

CREATE LIVE TABLE employee_silver (
  CONSTRAINT workday_id_valid     EXPECT (workday_id     IS NOT NULL),
  CONSTRAINT cost_center_valid    EXPECT (cost_center    IS NOT NULL),
  CONSTRAINT preferred_name_valid EXPECT (preferred_name IS NOT NULL),
  CONSTRAINT region_valid         EXPECT (region         IS NOT NULL  and length(region) = 4),
  CONSTRAINT country_valid        EXPECT (country        IS NOT NULL  and length(country) = 3),
  CONSTRAINT state_valid          EXPECT (state          IS NOT NULL  and length(state) = 2),
  CONSTRAINT office_valid         EXPECT (office         IS NOT NULL)
  ON VIOLATION FAIL UPDATE
)
COMMENT "The cleaned, standardized and validated version of the workday employee entity"
AS 
--USE douglas_moore_hr;
SELECT
  workday_id,
  cost_center,
  preferred_name,
  split(location,'[. ]')[0] as region,
  split(location,'[. ]')[1] as country,
  split(location,'[. ]')[2] as state,
  split(location,'[. ]')[3] as office
FROM live.workday_employee_bronze

-- COMMAND ----------

CREATE LIVE TABLE employee_stats_gold (
  CONSTRAINT valid_cnt EXPECT (cnt IS NOT NULL),
  CONSTRAINT valid_state EXPECT (state IS NOT NULL)
  ON VIOLATION FAIL UPDATE
)
COMMENT "Gold level summary stats"
AS 
--USE douglas_moore_hr;
SELECT
  count(*) cnt,
  cost_center,
  region,
  country,
  state,
  office
FROM live.employee_silver
GROUP BY 2,3,4,5,6
ORDER BY 2,3,4,5,6

-- COMMAND ----------


