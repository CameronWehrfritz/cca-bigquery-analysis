-- ================================================================
-- Monthly Usage Trends with Year-over-Year and Month-over-Month Analysis
-- Advanced analytics for identifying trends
-- File: queries/02_monthly_trends_analysis.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

-- ================================================================
-- Building up the window functions step by step
-- ================================================================

-- -- Step 1: Basic monthly aggregation
-- SELECT 
--   DATE_TRUNC(usage_date, MONTH) as usage_month,
--   customer_type,
--   SUM(kwh_used) as monthly_kwh,
--   COUNT(DISTINCT customer_id) as customers_active,
--   ROUND(SUM(kwh_used) / COUNT(DISTINCT customer_id), 2) as avg_usage_per_customer
-- FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
-- WHERE usage_date >= '2023-01-01'  -- Just 2023-2024 for now
-- GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- ORDER BY customer_type, usage_month;


-- -- Step 2: Add LAG function to the aggregated results
-- -- monthly aggregation, for further analysis
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh,
--     COUNT(DISTINCT customer_id) as customers_active,
--     ROUND(SUM(kwh_used) / COUNT(DISTINCT customer_id), 2) as avg_usage_per_customer
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'  -- 2+ years for YoY comparison
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   usage_month,
--   customer_type,
--   monthly_kwh as current_month_kwh,
--   customers_active,
--   avg_usage_per_customer,
  
--   -- Add LAG for year-over-year comparison (12 months ago)
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as previous_year_kwh,
  
--   -- Calculate YoY percentage change
--   ROUND(
--     (monthly_kwh - LAG(monthly_kwh, 12) OVER (
--       PARTITION BY customer_type 
--       ORDER BY usage_month
--     )) / NULLIF(LAG(monthly_kwh, 12) OVER (
--       PARTITION BY customer_type 
--       ORDER BY usage_month
--     ), 0) * 100, 2
--   ) as yoy_change_pct

-- FROM monthly_data
-- WHERE usage_month >= '2023-01-01'  -- Only show where YoY comparison is available
-- ORDER BY customer_type, usage_month;


-- Step 3: Add LAG functions to the aggregated results
-- calculate Year-over-Year and Month-over-Month changes
WITH monthly_data AS (
  -- Monthly aggregation from daily usage data
  SELECT
    DATE_TRUNC(usage_date, MONTH) as usage_month,
    customer_type,
    SUM(kwh_used) as monthly_kwh,
    COUNT(DISTINCT customer_id) as customers_active,
    ROUND(SUM(kwh_used) / COUNT(DISTINCT customer_id), 2) as avg_usage_per_customer
  FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
  WHERE usage_date >= '2022-01-01'  -- Include all data for LAG calculations
  GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
)

SELECT
  usage_month,
  customer_type,
  monthly_kwh as current_month_kwh,
  customers_active,
  avg_usage_per_customer,
 
  -- Year-over-year comparison: same month from previous year
  LAG(monthly_kwh, 12) OVER (
    PARTITION BY customer_type
    ORDER BY usage_month
  ) as previous_year_month_kwh,  -- Much clearer naming!
 
  -- Calculate YoY percentage change
  ROUND(
    (monthly_kwh - LAG(monthly_kwh, 12) OVER (
      PARTITION BY customer_type
      ORDER BY usage_month
    )) / NULLIF(LAG(monthly_kwh, 12) OVER (
      PARTITION BY customer_type
      ORDER BY usage_month
    ), 0) * 100, 2
  ) as yoy_change_pct,
 
  -- Month-over-month comparison: previous sequential month
  LAG(monthly_kwh, 1) OVER (
    PARTITION BY customer_type
    ORDER BY usage_month
  ) as previous_month_kwh,
 
  -- Calculate MoM percentage change
  ROUND(
    (monthly_kwh - LAG(monthly_kwh, 1) OVER (
      PARTITION BY customer_type
      ORDER BY usage_month
    )) / NULLIF(LAG(monthly_kwh, 1) OVER (
      PARTITION BY customer_type
      ORDER BY usage_month
    ), 0) * 100, 2
  ) as mom_change_pct

FROM monthly_data
WHERE usage_month >= '2022-01-01'  -- Show complete time series
ORDER BY customer_type, usage_month;

-- This pattern - using a CTE for aggregation followed by window functions - 
-- is essential for time-series analysis in data warehousing, especially for utility 
-- load forecasting and customer growth tracking.



-- -- ================================================================
-- -- Debug NULL Values in Year-over-Year Analysis
-- -- Let's see why we're getting NULLs
-- -- ================================================================

-- -- Check 1: Customer enrollment timing
-- SELECT 
--   EXTRACT(YEAR FROM enrollment_date) as enrollment_year,
--   EXTRACT(MONTH FROM enrollment_date) as enrollment_month,
--   COUNT(*) as customers_enrolled
-- FROM `cca-bigquery-analytics.cca_demo.customers`
-- GROUP BY enrollment_year, enrollment_month
-- ORDER BY enrollment_year, enrollment_month;

-- -- Check 2: Data availability by month for each customer type
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     COUNT(DISTINCT customer_id) as active_customers,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT
--   usage_month,
--   customer_type,
--   active_customers,
--   monthly_kwh,
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type
--     ORDER BY usage_month
--   ) as previous_year_kwh,
--   -- Check if we have data 12 months ago
--   CASE
--     WHEN LAG(monthly_kwh, 12) OVER (PARTITION BY customer_type ORDER BY usage_month) IS NULL
--     THEN 'Missing 12mo ago'
--     ELSE 'Has comparison'
--   END as data_status
-- FROM monthly_data
-- WHERE usage_month >= '2023-01-01'
-- ORDER BY customer_type, usage_month;



-- -- Test: Do we actually have 12 consecutive months of 2022 data?
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   customer_type,
--   COUNT(*) as months_of_data,
--   MIN(usage_month) as first_month,
--   MAX(usage_month) as last_month
-- FROM monthly_data
-- WHERE usage_month < '2023-01-01'  -- Only 2022 data
-- GROUP BY customer_type;

-- -- Test: Check if 2022 data has any NULL values
-- SELECT 
--   usage_month,
--   customer_type,
--   monthly_kwh,
--   CASE WHEN monthly_kwh IS NULL THEN 'NULL VALUE' ELSE 'HAS VALUE' END as value_status
-- FROM (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01' AND usage_date < '2023-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )
-- WHERE monthly_kwh IS NULL OR usage_month = '2022-01-01'
-- ORDER BY customer_type, usage_month;

-- -- Simple LAG test - let's see what's actually happening
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01' AND usage_date <= '2023-02-28'  -- Just 14 months
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   usage_month,
--   customer_type,
--   monthly_kwh,
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as lag_12_months,
--   ROW_NUMBER() OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as row_num
-- FROM monthly_data
-- WHERE customer_type = 'Large Commercial'  -- Just one customer type for clarity
-- ORDER BY usage_month;

-- -- Check if LAG is actually finding 2022 data but it's getting lost somewhere
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh,
--     COUNT(DISTINCT customer_id) as customers_active
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   usage_month,
--   customer_type,
--   monthly_kwh,
--   customers_active,
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as lag_value_raw
-- FROM monthly_data
-- WHERE (usage_month = '2022-01-01' OR usage_month = '2023-01-01')
--   AND customer_type = 'Large Commercial'
-- ORDER BY usage_month;


-- -- Check for missing months that would break the LAG sequence
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   usage_month,
--   customer_type,
--   ROW_NUMBER() OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as row_num,
--   monthly_kwh
-- FROM monthly_data
-- WHERE customer_type = 'Large Commercial'
--   AND usage_month BETWEEN '2022-01-01' AND '2023-02-01'
-- ORDER BY usage_month;


-- -- Test LAG on the exact same data structure
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )


-- -- Test
-- SELECT 
--   usage_month,
--   customer_type,
--   ROW_NUMBER() OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as row_num,
--   monthly_kwh,
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as lag_12_value
-- FROM monthly_data
-- WHERE customer_type = 'Large Commercial'
--   AND usage_month BETWEEN '2022-01-01' AND '2023-02-01'
-- ORDER BY usage_month;


-- -- Test
-- WITH monthly_data AS (
--   SELECT 
--     DATE_TRUNC(usage_date, MONTH) as usage_month,
--     customer_type,
--     SUM(kwh_used) as monthly_kwh,
--     COUNT(DISTINCT customer_id) as customers_active
--   FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
--   WHERE usage_date >= '2022-01-01'
--   GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
-- )

-- SELECT 
--   usage_month,
--   customer_type,
--   monthly_kwh,
--   customers_active,
--   LAG(monthly_kwh, 12) OVER (
--     PARTITION BY customer_type 
--     ORDER BY usage_month
--   ) as previous_year_kwh
-- FROM monthly_data
-- WHERE usage_month >= '2022-01-01'
-- ORDER BY customer_type, usage_month;

