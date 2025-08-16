-- ================================================================
-- Data Structure Exploration
-- Quick checks to understand table schemas and sample data
-- ================================================================

-- 1. Customers Table Sample
SELECT * 
FROM `cca-bigquery-analytics.cca_demo.customers`
LIMIT 10;

-- 2. Daily Usage Facts Sample (single partition for speed)
SELECT *
FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
WHERE usage_date = '2024-08-01'
LIMIT 10;

-- 3. Program Enrollments Sample  
SELECT *
FROM `cca-bigquery-analytics.cca_demo.program_enrollments`
LIMIT 10;

-- 4. Quick Record Counts 
SELECT 
  'customers' as table_name,
  COUNT(*) as total_records
FROM `cca-bigquery-analytics.cca_demo.customers`

UNION ALL

SELECT 
  'daily_usage_facts' as table_name,
  COUNT(*) as total_records  
FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`

UNION ALL

SELECT 
  'program_enrollments' as table_name,
  COUNT(*) as total_records
FROM `cca-bigquery-analytics.cca_demo.program_enrollments`;