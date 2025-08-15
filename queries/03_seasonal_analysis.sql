-- ================================================================
-- Seasonal Usage Patterns with Moving Averages
-- Advanced analytics for identifying trends and anomalies
-- File: queries/03_seasonal_analysis.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

-- -- Step 1: 
-- SELECT
-- usage_date,
-- customer_type,
-- SUM(kwh_used) as daily_total_kwh,
-- COUNT(DISTINCT customer_id) as active_customers,
-- AVG(temperature_high_f) as avg_temperature,
-- MAX(is_weekend) as is_weekend,
-- MAX(is_holiday) as is_holiday,

-- -- Season classification
-- CASE 
--     WHEN EXTRACT(MONTH FROM usage_date) IN (12, 1, 2) THEN 'Winter'
--     WHEN EXTRACT(MONTH FROM usage_date) IN (3, 4, 5) THEN 'Spring' 
--     WHEN EXTRACT(MONTH FROM usage_date) IN (6, 7, 8) THEN 'Summer'
--     ELSE 'Fall'
-- END as season

-- FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
-- WHERE usage_date >= '2024-01-01'  -- Focus on current year for patterns
-- GROUP BY usage_date, customer_type;

WITH daily_metrics AS (
  SELECT 
    usage_date,
    customer_type,
    SUM(kwh_used) as daily_total_kwh,
    COUNT(DISTINCT customer_id) as active_customers,
    AVG(temperature_high_f) as avg_temperature,
    MAX(is_weekend) as is_weekend,
    MAX(is_holiday) as is_holiday,
    
    -- Season classification
    CASE 
      WHEN EXTRACT(MONTH FROM usage_date) IN (12, 1, 2) THEN 'Winter'
      WHEN EXTRACT(MONTH FROM usage_date) IN (3, 4, 5) THEN 'Spring' 
      WHEN EXTRACT(MONTH FROM usage_date) IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END as season
    
  FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
  WHERE usage_date >= '2024-01-01'  -- Focus on current year for patterns
  GROUP BY usage_date, customer_type
)

SELECT 
  usage_date,
  customer_type,
  daily_total_kwh,
  active_customers,
  ROUND(daily_total_kwh / active_customers, 2) as avg_usage_per_customer,
  avg_temperature,
  FORMAT_DATE('%A', usage_date) as day_of_week,
  is_weekend,
  is_holiday,
  season,
  
  -- 7-day moving average (smooths out weekly volatility)
  ROUND(AVG(daily_total_kwh) OVER (
    PARTITION BY customer_type
    ORDER BY usage_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) as seven_day_moving_avg,
  
  -- 30-day moving average (smooths out monthly patterns)
  ROUND(AVG(daily_total_kwh) OVER (
    PARTITION BY customer_type
    ORDER BY usage_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 2) as thirty_day_moving_avg,
  
  -- Compare daily usage to 7-day average (spot anomalies)
  ROUND(
    (daily_total_kwh - AVG(daily_total_kwh) OVER (
      PARTITION BY customer_type
      ORDER BY usage_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )) / NULLIF(AVG(daily_total_kwh) OVER (
      PARTITION BY customer_type
      ORDER BY usage_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0) * 100, 1
  ) as daily_vs_weekly_moving_avg_pct,
  
  -- Temperature correlation indicator
  CASE
    WHEN avg_temperature > 85 THEN 'Hot Day (AC Load)'
    WHEN avg_temperature < 50 THEN 'Cold Day (Heating Load)'
    ELSE 'Mild Weather'
  END as weather_impact,
  
--   -- Weekend vs weekday usage comparison
--   -- The 15-day window is short enough that it won't span season boundaries, but long enough to 
--   -- capture meaningful weekday patterns (about 10-11 weekdays in that window).
--   -- This gives you a fairly clean "How does today compare to recent weekdays in this season?" metric
--   -- without the cross-seasonal contamination issue.
--   ROUND(
--     daily_total_kwh - AVG(CASE WHEN NOT is_weekend THEN daily_total_kwh END) OVER (
--       PARTITION BY customer_type, season
--       ORDER BY usage_date
--       ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
--     ), 0
--   ) as daily_vs_weekday_baseline

FROM daily_metrics
ORDER BY customer_type, usage_date;
