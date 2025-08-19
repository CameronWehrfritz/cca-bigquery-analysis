-- ================================================================
-- Monthly Usage Trends with Automated Alerting Logic
-- Modification of original query for Cloud Function integration
-- File: queries/04_monthly_trends_alert.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-16
-- ================================================================

WITH monthly_data AS (
  -- Monthly aggregation from daily usage data
  SELECT
    DATE_TRUNC(usage_date, MONTH) as usage_month,
    customer_type,
    ROUND(SUM(kwh_used), 2) as monthly_kwh,
    COUNT(DISTINCT customer_id) as customers_active,
    ROUND(SUM(kwh_used) / COUNT(DISTINCT customer_id), 2) as avg_usage_per_customer
  FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
  WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH)
  GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
),

trend_analysis AS (
  SELECT
    usage_month,
    customer_type,
    monthly_kwh as current_month_kwh,
    customers_active,
    avg_usage_per_customer,
   
    -- Year-over-year comparison
    LAG(monthly_kwh, 12) OVER (
      PARTITION BY customer_type
      ORDER BY usage_month
    ) as previous_year_month_kwh,
   
    -- Calculate year-over-year percentage change
    ROUND(
      (monthly_kwh - LAG(monthly_kwh, 12) OVER (
        PARTITION BY customer_type
        ORDER BY usage_month
      )) / NULLIF(LAG(monthly_kwh, 12) OVER (
        PARTITION BY customer_type
        ORDER BY usage_month
      ), 0) * 100, 2
    ) as yoy_change_pct
  FROM monthly_data
),

-- Alert logic: Check most recent complete month
alerts AS (
  SELECT 
    *,
    CASE 
      WHEN yoy_change_pct > 60 THEN 'HIGH_GROWTH_ALERT'
      WHEN yoy_change_pct < 10 THEN 'LOW_GROWTH_ALERT'
      WHEN yoy_change_pct IS NULL THEN 'INSUFFICIENT_DATA'
      ELSE 'NORMAL'
    END as alert_status,
    CASE 
      WHEN yoy_change_pct > 60 THEN 'CRITICAL'
      WHEN yoy_change_pct < 10 THEN 'WARNING'
      ELSE 'INFO'
    END as alert_severity
  FROM trend_analysis
--   WHERE usage_month = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
    WHERE usage_month = '2024-08-01' -- test
    AND yoy_change_pct IS NOT NULL
)

-- Only return rows that need alerts
SELECT 
  usage_month,
  customer_type,
  current_month_kwh,
  customers_active,
  yoy_change_pct,
  alert_status,
  alert_severity,
  CONCAT(
    customer_type, ' segment: ', 
    CAST(ROUND(yoy_change_pct, 1) AS STRING), '% YoY growth (',
    CAST(ROUND(current_month_kwh, 0) AS STRING), ' kWh total)'
  ) as alert_message
FROM alerts 
WHERE alert_status IN ('HIGH_GROWTH_ALERT', 'LOW_GROWTH_ALERT')
ORDER BY 
  CASE alert_severity 
    WHEN 'CRITICAL' THEN 1 
    WHEN 'WARNING' THEN 2 
    ELSE 3 
  END,
  customer_type;