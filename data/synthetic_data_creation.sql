-- ================================================================
-- CCA BigQuery Analytics: Synthetic Data Creation
-- Creates realistic Community Choice Aggregator operational data
-- Optimized for BigQuery with partitioning and clustering
-- ~40M usage records across 50K customers
-- ================================================================

-- ================================================================
-- BATCH 1: Create Customers Table
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.customers` AS
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


-- ================================================================
-- BATCH 2: Create Daily Usage Facts Table (Structure Only)
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.daily_usage_facts` (
  usage_date DATE,
  customer_id STRING,
  kwh_used FLOAT64,
  peak_demand_kw FLOAT64,
  cost_dollars FLOAT64,
  rate_plan STRING,
  customer_type STRING,
  city STRING,
  temperature_high_f INT64,
  is_weekend BOOL,
  is_holiday BOOL
)
PARTITION BY usage_date
CLUSTER BY customer_type, city
OPTIONS(
  description="Daily energy usage facts with realistic consumption patterns",
  partition_expiration_days=2555  -- ~7 years retention
);

-- ================================================================
-- BATCH 3: Test Usage Data Insert (Small Sample)
-- ================================================================

INSERT INTO `cca-bigquery-analytics.cca_demo.daily_usage_facts`
SELECT 
  usage_date,
  customer_id,
  
  -- Realistic usage calculation
  GREATEST(1.0,  -- Minimum 1 kWh usage
    CASE customer_type
      WHEN 'Residential' THEN 
        25.0 
        + 12.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
        + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN 3.0 ELSE 0 END
        + (RAND() - 0.5) * 8.0
        + CASE WHEN has_ev THEN 12.0 ELSE 0 END
        - CASE WHEN has_solar THEN 8.0 ELSE 0 END
        
      WHEN 'Small Commercial' THEN 
        120.0
        + 30.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
        + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN -25.0 ELSE 0 END
        + (RAND() - 0.5) * 40.0
        
      ELSE -- Large Commercial
        800.0
        + 150.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
        + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN -50.0 ELSE 0 END
        + (RAND() - 0.5) * 200.0
    END
  ) as kwh_used,
  
  -- Peak demand (simplified calculation)
  25.0 * 0.06 as peak_demand_kw,
  
  -- Cost calculation (simplified)
  25.0 * 
  CASE rate_plan
    WHEN 'ECO100' THEN 0.23
    WHEN 'ECOplus' THEN 0.21
    ELSE 0.19
  END as cost_dollars,
  
  -- Denormalized attributes
  rate_plan,
  customer_type,
  city,
  75 as temperature_high_f,
  EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) as is_weekend,
  false as is_holiday

FROM (
  -- Cross join dates with customers
  SELECT 
    date_val as usage_date,
    c.customer_id,
    c.customer_type,
    c.city,
    c.rate_plan,
    c.has_ev,
    c.has_solar,
    c.enrollment_date
  FROM UNNEST(GENERATE_DATE_ARRAY('2024-08-01', '2024-08-07')) as date_val
  CROSS JOIN (
    SELECT * 
    FROM `cca-bigquery-analytics.cca_demo.customers` 
    LIMIT 1000
  ) c
  WHERE date_val >= c.enrollment_date
);

-- Check partition information
SELECT 
  table_name,
  partition_id,
  total_rows,
  total_logical_bytes
FROM `cca-bigquery-analytics.cca_demo.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'daily_usage_facts'
  AND partition_id IS NOT NULL  -- Exclude the unpartitioned __NULL__ partition
ORDER BY partition_id;

-- ================================================================
-- BATCH 4: Full Scale Usage Data
-- ================================================================

INSERT INTO `cca-bigquery-analytics.cca_demo.daily_usage_facts`
SELECT 
  usage_date,
  customer_id,
  kwh_used,
  
  -- Now we can reference kwh_used from the subquery
  kwh_used * (0.055 + 0.025 * RAND()) as peak_demand_kw,
  
  -- Cost calculation
  kwh_used * 
  CASE rate_plan
    WHEN 'ECO100' THEN 0.23
    WHEN 'ECOplus' THEN 0.21
    ELSE 0.19
  END as cost_dollars,
  
  rate_plan,
  customer_type,
  city,
  temperature_high_f,
  is_weekend,
  is_holiday

FROM (
  -- Subquery calculates kwh_used first
  SELECT 
    usage_date,
    customer_id,
    rate_plan,
    customer_type,
    city,
    temperature_high_f,
    EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) as is_weekend,
    is_holiday,
    
    -- Calculate kwh_used in subquery
    GREATEST(1.0,
      CASE customer_type
        WHEN 'Residential' THEN 
          25.0 
          + 12.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
          + CASE 
              WHEN temperature_high_f > 85 THEN (temperature_high_f - 85) * 0.3
              WHEN temperature_high_f < 50 THEN (50 - temperature_high_f) * 0.2
              ELSE 0 
            END
          + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN 3.0 ELSE 0 END
          + (RAND() - 0.5) * 8.0
          + CASE WHEN has_ev THEN 12.0 + RAND() * 8.0 ELSE 0 END
          - CASE WHEN has_solar THEN 8.0 + RAND() * 6.0 ELSE 0 END
          
        WHEN 'Small Commercial' THEN 
          120.0
          + 30.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
          + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN -25.0 ELSE 0 END
          + (RAND() - 0.5) * 40.0
          
        ELSE -- Large Commercial
          800.0
          + 150.0 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM usage_date) / 365.25)
          + CASE WHEN EXTRACT(DAYOFWEEK FROM usage_date) IN (1, 7) THEN -50.0 ELSE 0 END
          + (RAND() - 0.5) * 200.0
      END
    ) as kwh_used
    
  FROM (
    -- Generate all combinations
    SELECT 
      date_val as usage_date,
      c.customer_id,
      c.customer_type,
      c.city,
      c.rate_plan,
      c.has_ev,
      c.has_solar,
      c.enrollment_date,
      CAST(
        62 + 18 * SIN(2 * 3.14159 * EXTRACT(DAYOFYEAR FROM date_val) / 365.25)
        + (RAND() - 0.5) * 20
      AS INT64) as temperature_high_f,
      date_val IN (
        '2022-01-01', '2022-07-04', '2022-11-24', '2022-12-25',
        '2023-01-01', '2023-07-04', '2023-11-23', '2023-12-25',
        '2024-01-01', '2024-07-04', '2024-11-28', '2024-12-25'
      ) as is_holiday
      
    FROM UNNEST(GENERATE_DATE_ARRAY('2022-01-01', '2024-08-31')) as date_val
    CROSS JOIN `cca-bigquery-analytics.cca_demo.customers` c
    WHERE date_val >= c.enrollment_date
  )
);

-- Check Partitioning at Scale (should show ~900 partitions!)
SELECT 
  COUNT(DISTINCT partition_id) as total_partitions,
  MIN(partition_id) as earliest_partition,
  MAX(partition_id) as latest_partition,
  ROUND(AVG(total_rows), 0) as avg_rows_per_partition
FROM `cca-bigquery-analytics.cca_demo.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'daily_usage_facts'
  AND partition_id IS NOT NULL;

-- ================================================================
-- Create Program Enrollments Table Structure
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.program_enrollments` (
  customer_id STRING,
  program_name STRING,
  program_category STRING,
  program_enrollment_date DATE,
  incentive_amount FLOAT64,
  program_status STRING,
  program_description STRING
)
OPTIONS(
  description="Customer participation in CCA programs and incentives"
);

-- ================================================================
-- BATCH 5: Program Enrollments
-- Realistic CCA program participation
-- ================================================================

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


-- ================================================================
-- BATCH 6: Create Data Quality and Summary Statistics Table
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.data_summary` AS
SELECT
  'customers' as table_name,
  COUNT(*) as record_count,
  MIN(enrollment_date) as min_date,
  MAX(enrollment_date) as max_date,
  'Customer master data' as description
FROM `cca-bigquery-analytics.cca_demo.customers`

UNION ALL

SELECT
  'daily_usage_facts' as table_name,
  COUNT(*) as record_count,
  MIN(usage_date) as min_date,
  MAX(usage_date) as max_date,
  'Daily energy usage transactions' as description
FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`

UNION ALL

SELECT
  'program_enrollments' as table_name,
  COUNT(*) as record_count,
  MIN(program_enrollment_date) as min_date,
  MAX(program_enrollment_date) as max_date,
  'Customer program participation' as description
FROM `cca-bigquery-analytics.cca_demo.program_enrollments`;

-- Check summary table
SELECT * FROM cca-bigquery-analytics.cca_demo.data_summary
LIMIT 100;

-- ================================================================
-- SETUP COMPLETE!
-- ================================================================