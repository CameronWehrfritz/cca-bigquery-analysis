-- ================================================================
-- Monthly Usage Trends with Year-over-Year and Month-over-Month Analysis
-- Advanced analytics for identifying trends
-- File: queries/01_monthly_trends_analysis.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

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
