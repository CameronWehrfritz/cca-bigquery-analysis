-- ================================================================
-- Seasonal Usage Patterns with Moving Averages
-- Advanced analytics for identifying trends and anomalies
-- File: queries/03_seasonal_analysis.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

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
  ROUND(daily_total_kwh, 2) as daily_total_kwh,
  active_customers,
  ROUND(daily_total_kwh / active_customers, 2) as avg_usage_per_customer,
  ROUND(avg_temperature, 2) as avg_temperature,
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
  
  -- Weekend vs weekday usage comparison (removed due to cross-seasonal contamination)
  -- daily_vs_weekday_baseline would require smaller window to prevent season boundary issues

FROM daily_metrics
ORDER BY customer_type, usage_date;
