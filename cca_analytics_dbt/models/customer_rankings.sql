-- ================================================================
-- Customer Usage Rankings and Percentile Analysis
-- Advanced customer segmentation for utility account management
-- dbt model: models/customer_rankings.sql
-- Author: Cameron Wehrfritz
-- Created: 2025-08-19
-- Updated: 2025-08-22 - Updated with explicit units (kwh and dollars)
-- ================================================================

{{ config(materialized='table') }}

WITH customer_metrics AS (
  SELECT
    u.customer_id,
    c.customer_type,
    c.city,
    c.rate_plan,
    c.has_solar,
    c.has_ev,
    c.has_battery_storage,
    c.is_low_income_qualified,
    
    -- Usage metrics (YTD 2024)
    COUNT(u.usage_date) as days_with_usage,
    ROUND(SUM(u.kwh_used), 2) as total_kwh_ytd,
    ROUND(AVG(u.kwh_used), 2) as avg_daily_kwh,
    ROUND(STDDEV(u.kwh_used), 2) as usage_volatility_kwh,
    ROUND(MAX(u.kwh_used), 2) as peak_daily_usage_kwh,
    ROUND(MIN(u.kwh_used),2 ) as min_daily_usage_kwh,

    -- Financial metrics
    ROUND(SUM(u.cost_dollars), 2) as total_cost_ytd,
    ROUND(AVG(u.cost_dollars), 2) as avg_daily_cost_dollars,
    ROUND(SUM(u.cost_dollars) / SUM(u.kwh_used), 4) as effective_dollars_per_kwh,
    
    -- Seasonal patterns
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM u.usage_date) IN (12,1,2) THEN u.kwh_used END), 2) as avg_winter_usage_kwh,
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM u.usage_date) IN (6,7,8) THEN u.kwh_used END), 2) as avg_summer_usage_kwh,
    
    -- Weekend vs weekday patterns  
    ROUND(AVG(CASE WHEN u.is_weekend THEN u.kwh_used END), 2) as avg_weekend_usage_kwh,
    ROUND(AVG(CASE WHEN NOT u.is_weekend THEN u.kwh_used END), 2) as avg_weekday_usage_kwh
    
  FROM {{ source('cca_demo', 'daily_usage_facts') }} u
  JOIN {{ source('cca_demo', 'customers') }} c 
    ON u.customer_id = c.customer_id
  WHERE u.usage_date >= '2024-01-01'  -- YTD analysis
  GROUP BY u.customer_id, c.customer_type, c.city, c.rate_plan, 
           c.has_solar, c.has_ev, c.has_battery_storage, c.is_low_income_qualified
)

SELECT 
  customer_id,
  customer_type,
  city,
  rate_plan,
  has_solar,
  has_ev,
  has_battery_storage,
  is_low_income_qualified,
  
  -- Usage metrics
  days_with_usage,
  total_kwh_ytd,
  avg_daily_kwh,
  usage_volatility_kwh,
  peak_daily_usage_kwh,
  min_daily_usage_kwh,
  
  -- Financial metrics
  total_cost_ytd,
  avg_daily_cost_dollars,
  effective_dollars_per_kwh,
  
  -- Seasonal analysis
  avg_winter_usage_kwh,
  avg_summer_usage_kwh,
  ROUND(
    CASE 
      WHEN avg_summer_usage_kwh > 0 
      THEN (avg_winter_usage_kwh - avg_summer_usage_kwh) / avg_summer_usage_kwh * 100 
      ELSE NULL 
    END, 1
  ) as winter_vs_summer_pct,
  
  -- Weekend vs weekday analysis
  avg_weekend_usage_kwh,
  avg_weekday_usage_kwh,
  ROUND(
    CASE 
      WHEN avg_weekday_usage_kwh > 0 
      THEN (avg_weekend_usage_kwh - avg_weekday_usage_kwh) / avg_weekday_usage_kwh * 100 
      ELSE NULL 
    END, 1
  ) as weekend_vs_weekday_pct,
  
  -- Overall rankings
  ROW_NUMBER() OVER (ORDER BY total_kwh_ytd DESC) as usage_rank_overall,
  ROUND(PERCENT_RANK() OVER (ORDER BY total_kwh_ytd) * 100, 1) as usage_percentile_overall,
  
  -- Rankings within customer segment
  ROW_NUMBER() OVER (
    PARTITION BY customer_type 
    ORDER BY total_kwh_ytd DESC
  ) as usage_rank_in_segment,
  
  ROUND(PERCENT_RANK() OVER (
    PARTITION BY customer_type 
    ORDER BY total_kwh_ytd
  ) * 100, 1) as usage_percentile_in_segment,
  
  -- Rankings within city
  ROW_NUMBER() OVER (
    PARTITION BY city 
    ORDER BY total_kwh_ytd DESC
  ) as usage_rank_in_city,
  
  -- Revenue rankings (important for utility)
  ROW_NUMBER() OVER (ORDER BY total_cost_ytd DESC) as revenue_rank,
  ROUND(PERCENT_RANK() OVER (ORDER BY total_cost_ytd) * 100, 1) as revenue_percentile,
  
  -- Usage efficiency ranking (kwh per dollar)
  ROW_NUMBER() OVER (ORDER BY effective_dollars_per_kwh ASC) as efficiency_rank,
  
  -- Volatility ranking (risk assessment)
  NTILE(5) OVER (ORDER BY usage_volatility_kwh) as volatility_quintile,
  
  -- Customer value segments
  CASE 
    WHEN total_kwh_ytd >= PERCENTILE_CONT(total_kwh_ytd, 0.9) OVER (PARTITION BY customer_type) 
      THEN 'High Usage'
    WHEN total_kwh_ytd >= PERCENTILE_CONT(total_kwh_ytd, 0.7) OVER (PARTITION BY customer_type) 
      THEN 'Above Average'
    WHEN total_kwh_ytd >= PERCENTILE_CONT(total_kwh_ytd, 0.3) OVER (PARTITION BY customer_type) 
      THEN 'Average'
    WHEN total_kwh_ytd >= PERCENTILE_CONT(total_kwh_ytd, 0.1) OVER (PARTITION BY customer_type) 
      THEN 'Below Average'
    ELSE 'Low Usage'
  END as usage_segment,
  
  -- Risk indicators
  CASE 
    WHEN avg_daily_kwh < 5 THEN 'Potential Churn Risk'
    WHEN days_with_usage < 200 THEN 'Irregular Usage'
    WHEN usage_volatility_kwh > avg_daily_kwh * 2 THEN 'High Volatility'
    ELSE 'Stable Customer'
  END as risk_indicator

FROM customer_metrics
ORDER BY total_kwh_ytd DESC