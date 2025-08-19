-- ================================================================
-- CCA BigQuery Analytics: Data Summary Statistics Table
-- Creates summary statistics and data quality metrics
-- File: data_setup/05_create_summary_stats.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-19
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