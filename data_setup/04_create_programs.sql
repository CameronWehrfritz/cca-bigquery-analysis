-- ================================================================
-- CCA BigQuery Analytics: Program Enrollments Table
-- Creates customer program participation and incentive data
-- File: data_setup/04_create_programs.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-19
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.program_enrollments` (
  customer_id STRING NOT NULL,
  program_name STRING NOT NULL,
  program_category STRING,
  program_enrollment_date DATE NOT NULL,
  incentive_amount FLOAT64,
  program_status STRING,
  program_description STRING,
  PRIMARY KEY (customer_id, program_name, program_enrollment_date) NOT ENFORCED,
  FOREIGN KEY (customer_id) REFERENCES `cca-bigquery-analytics.cca_demo.customers`(customer_id) NOT ENFORCED
)
OPTIONS(
  description="Customer participation in CCA programs and incentives - Primary Key: (customer_id, program_name, program_enrollment_date)"
);

-- Populate with realistic program participation data
INSERT INTO `cca-bigquery-analytics.cca_demo.program_enrollments`

-- Solar rebate program
SELECT 
  customer_id,
  'Solar Rebate' as program_name,
  'Distributed Energy Resources' as program_category,
  DATE_ADD(enrollment_date, INTERVAL CAST(30 + RAND() * 700 AS INT64) DAY) as program_enrollment_date,
  ROUND(2000 + RAND() * 3000, 2) as incentive_amount,
  'Active' as program_status,
  'Solar panel installation incentive' as program_description
FROM `cca-bigquery-analytics.cca_demo.customers` 
WHERE has_solar = true

UNION ALL

-- EV charging rebate program  
SELECT 
  customer_id,
  'EV Charging Rebate' as program_name,
  'Transportation Electrification' as program_category,
  DATE_ADD(enrollment_date, INTERVAL CAST(30 + RAND() * 700 AS INT64) DAY) as program_enrollment_date,
  ROUND(500 + RAND() * 1000, 2) as incentive_amount,
  'Active' as program_status,
  'Electric vehicle charging station rebate' as program_description
FROM `cca-bigquery-analytics.cca_demo.customers` 
WHERE has_ev = true

UNION ALL

-- Energy efficiency program
SELECT 
  customer_id,
  'Energy Efficiency' as program_name,
  'Demand Management' as program_category,
  DATE_ADD(enrollment_date, INTERVAL CAST(60 + RAND() * 600 AS INT64) DAY) as program_enrollment_date,
  ROUND(200 + RAND() * 800, 2) as incentive_amount,
  CASE WHEN RAND() < 0.9 THEN 'Active' ELSE 'Completed' END as program_status,
  'Home energy efficiency upgrades' as program_description
FROM `cca-bigquery-analytics.cca_demo.customers` 
WHERE customer_type = 'Residential' AND RAND() < 0.25  -- 25% participation

UNION ALL

-- Battery storage program
SELECT 
  customer_id,
  'Battery Storage' as program_name,
  'Grid Resilience' as program_category,
  DATE_ADD(enrollment_date, INTERVAL CAST(90 + RAND() * 800 AS INT64) DAY) as program_enrollment_date,
  ROUND(3000 + RAND() * 5000, 2) as incentive_amount,
  'Active' as program_status,
  'Home battery storage system incentive' as program_description
FROM `cca-bigquery-analytics.cca_demo.customers` 
WHERE has_battery_storage = true

UNION ALL

-- Low-income assistance program
SELECT 
  customer_id,
  'Energy Assistance' as program_name,
  'Community Benefits' as program_category,
  DATE_ADD(enrollment_date, INTERVAL CAST(15 + RAND() * 180 AS INT64) DAY) as program_enrollment_date,
  ROUND(300 + RAND() * 400, 2) as incentive_amount,
  'Active' as program_status,
  'Low-income customer energy assistance' as program_description
FROM `cca-bigquery-analytics.cca_demo.customers` 
WHERE is_low_income_qualified = true AND RAND() < 0.6;  -- 60% enrollment rate