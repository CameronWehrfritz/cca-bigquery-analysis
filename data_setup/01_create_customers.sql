-- ================================================================
-- CCA BigQuery Analytics: Customer Dimension Table Creation
-- Creates synthetic customer master data for CCA analytics
-- File: data_setup/01_create_customers.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-19
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.customers` 
(
  customer_id STRING NOT NULL,
  customer_type STRING,
  city STRING,
  rate_plan STRING,
  is_low_income_qualified BOOL,
  has_solar BOOL,
  has_ev BOOL,
  has_battery_storage BOOL,
  enrollment_date DATE,
  PRIMARY KEY (customer_id) NOT ENFORCED
)
AS
SELECT 
  -- Customer identifiers
  CONCAT('CUST_', LPAD(CAST(customer_num AS STRING), 6, '0')) as customer_id,
  
  -- Customer segmentation (realistic distribution)
  CASE 
    WHEN RAND() < 0.75 THEN 'Residential'
    WHEN RAND() < 0.95 THEN 'Small Commercial' 
    ELSE 'Large Commercial'
  END as customer_type,
  
  -- Geographic distribution (San Mateo County cities)
  CASE 
    WHEN RAND() < 0.25 THEN 'San Mateo'
    WHEN RAND() < 0.40 THEN 'Redwood City'  
    WHEN RAND() < 0.55 THEN 'Palo Alto'
    WHEN RAND() < 0.68 THEN 'Foster City'
    WHEN RAND() < 0.80 THEN 'Menlo Park'
    WHEN RAND() < 0.90 THEN 'East Palo Alto'
    WHEN RAND() < 0.96 THEN 'Burlingame'
    ELSE 'San Carlos'
  END as city,
  
  -- Rate plan distribution
  CASE
    WHEN RAND() < 0.15 THEN 'ECO100'    -- 100% renewable (premium)
    WHEN RAND() < 0.45 THEN 'ECOplus'   -- 50% renewable (mid-tier)
    ELSE 'ECO'                          -- Default clean energy
  END as rate_plan,
  
  -- Customer characteristics
  RAND() < 0.18 as is_low_income_qualified,
  RAND() < 0.12 as has_solar,
  RAND() < 0.15 as has_ev,
  RAND() < 0.08 as has_battery_storage,
  
  -- Enrollment timing
  DATE_ADD('2020-01-01', INTERVAL CAST(RAND() * 1460 AS INT64) DAY) as enrollment_date

FROM UNNEST(GENERATE_ARRAY(1, 50000)) as customer_num;