-- ================================================================
-- Advanced Multi-Table Analysis with Complex JOINs
-- Comprehensive customer lifecycle and program effectiveness analysis
-- File: queries/05_advanced_multi_table_analysis.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

WITH first_program_dates AS (
  -- Get the first program enrollment date for each customer
  SELECT 
    customer_id,
    MIN(program_enrollment_date) as first_program_date
  FROM `cca-bigquery-analytics.cca_demo.program_enrollments`
  GROUP BY customer_id
),

customer_usage_summary AS (
  -- Aggregate usage metrics by customer and time periods
  SELECT 
    u.customer_id,
    MIN(u.usage_date) as first_usage_date,
    MAX(u.usage_date) as last_usage_date,
    COUNT(DISTINCT u.usage_date) as total_usage_days,
    
    -- Overall usage metrics
    SUM(u.kwh_used) as lifetime_kwh,
    SUM(u.cost_dollars) as lifetime_revenue,
    ROUND(AVG(u.kwh_used), 2) as avg_daily_kwh,
    ROUND(STDDEV(u.kwh_used), 2) as usage_volatility_kwh,
    
    -- Pre-program vs post-program periods (using program enrollment as divider)
    SUM(CASE WHEN u.usage_date < COALESCE(fp.first_program_date, '2025-01-01') 
             THEN u.kwh_used ELSE 0 END) as pre_program_kwh,
    SUM(CASE WHEN u.usage_date >= COALESCE(fp.first_program_date, '2025-01-01') 
             THEN u.kwh_used ELSE 0 END) as post_program_kwh,
    
    COUNT(CASE WHEN u.usage_date < COALESCE(fp.first_program_date, '2025-01-01') 
               THEN u.usage_date END) as pre_program_days,
    COUNT(CASE WHEN u.usage_date >= COALESCE(fp.first_program_date, '2025-01-01') 
               THEN u.usage_date END) as post_program_days,
    
    -- Seasonal usage patterns
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM u.usage_date) IN (12,1,2) 
             THEN u.kwh_used END), 2) as avg_winter_kwh,
    ROUND(AVG(CASE WHEN EXTRACT(MONTH FROM u.usage_date) IN (6,7,8) 
             THEN u.kwh_used END), 2) as avg_summer_kwh,
             
    -- Recent vs historical usage (2024 vs pre-2024)
    ROUND(AVG(CASE WHEN u.usage_date >= '2024-01-01' 
             THEN u.kwh_used END), 2) as avg_recent_kwh,
    ROUND(AVG(CASE WHEN u.usage_date < '2024-01-01' 
             THEN u.kwh_used END), 2) as avg_historical_kwh
             
  FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts` u
  LEFT JOIN first_program_dates fp ON u.customer_id = fp.customer_id
  GROUP BY u.customer_id
),

program_analysis AS (
  -- Analyze program participation patterns
  SELECT 
    p.customer_id,
    COUNT(DISTINCT p.program_name) as programs_enrolled,
    STRING_AGG(DISTINCT p.program_name ORDER BY p.program_name) as program_list,
    MIN(p.program_enrollment_date) as first_program_date,
    MAX(p.program_enrollment_date) as latest_program_date,
    
    -- Program-specific enrollments
    COUNTIF(p.program_name = 'Solar Incentive Program') as solar_programs,
    COUNTIF(p.program_name = 'EV Charging Rebate') as ev_programs,
    COUNTIF(p.program_name = 'Energy Efficiency Rebate') as efficiency_programs,
    COUNTIF(p.program_name = 'Low-Income Energy Assistance') as assistance_programs,
    
    -- Enrollment timing analysis
    DATE_DIFF(MAX(p.program_enrollment_date), MIN(p.program_enrollment_date), DAY) as program_enrollment_span_days
    
  FROM `cca-bigquery-analytics.cca_demo.program_enrollments` p
  GROUP BY p.customer_id
),

city_benchmarks AS (
  -- Calculate city-level benchmarks for comparison
  SELECT DISTINCT
    c.city,
    c.customer_type,
    COUNT(*) OVER (PARTITION BY c.city, c.customer_type) as customers_in_segment,
    ROUND(AVG(u.lifetime_kwh) OVER (PARTITION BY c.city, c.customer_type), 2) as city_avg_lifetime_kwh,
    ROUND(AVG(u.avg_daily_kwh) OVER (PARTITION BY c.city, c.customer_type), 2) as city_avg_daily_kwh,
    ROUND(PERCENTILE_CONT(u.lifetime_kwh, 0.5) OVER (PARTITION BY c.city, c.customer_type), 2) as city_median_lifetime_kwh
  FROM `cca-bigquery-analytics.cca_demo.customers` c
  JOIN customer_usage_summary u ON c.customer_id = u.customer_id
)

-- Main query combining all dimensions
SELECT 
  c.customer_id,
  c.customer_type,
  c.city,
  c.rate_plan,
  c.enrollment_date as customer_enrollment_date,
  c.has_solar,
  c.has_ev,
  c.has_battery_storage,
  c.is_low_income_qualified,
  
  -- Usage lifecycle metrics
  u.first_usage_date,
  u.last_usage_date,
  DATE_DIFF(u.last_usage_date, u.first_usage_date, DAY) as customer_tenure_days,
  u.total_usage_days,
  ROUND(u.total_usage_days / NULLIF(DATE_DIFF(u.last_usage_date, u.first_usage_date, DAY), 0) * 100, 1) as usage_consistency_pct,
  
  -- Financial and usage metrics
  u.lifetime_kwh,
  u.lifetime_revenue,
  ROUND(u.lifetime_revenue / NULLIF(u.lifetime_kwh, 0), 4) as lifetime_avg_rate,
  u.avg_daily_kwh,
  u.usage_volatility_kwh,
  
  -- Program participation analysis
  COALESCE(p.programs_enrolled, 0) as programs_enrolled,
  p.program_list,
  p.first_program_date,
  p.latest_program_date,
  
  -- Program impact analysis (before vs after enrollment)
  CASE 
    WHEN p.first_program_date IS NOT NULL AND u.pre_program_days > 30 AND u.post_program_days > 30
    THEN ROUND((u.post_program_kwh / NULLIF(u.post_program_days, 0)) - 
               (u.pre_program_kwh / NULLIF(u.pre_program_days, 0)), 2)
    ELSE NULL
  END as daily_kwh_change_post_program,
  
  CASE 
    WHEN p.first_program_date IS NOT NULL AND u.pre_program_days > 30 AND u.post_program_days > 30
    THEN ROUND(((u.post_program_kwh / NULLIF(u.post_program_days, 0)) - 
                (u.pre_program_kwh / NULLIF(u.pre_program_days, 0))) /
               NULLIF((u.pre_program_kwh / NULLIF(u.pre_program_days, 0)), 0) * 100, 1)
    ELSE NULL
  END as pct_change_post_program,
  
  -- Seasonal analysis
  u.avg_winter_kwh,
  u.avg_summer_kwh,
  ROUND(
    CASE WHEN u.avg_summer_kwh > 0 
         THEN (u.avg_winter_kwh - u.avg_summer_kwh) / u.avg_summer_kwh * 100 
         ELSE NULL END, 1
  ) as winter_vs_summer_pct,
  
  -- Recent trend analysis
  u.avg_recent_kwh,
  u.avg_historical_kwh,
  ROUND(
    CASE WHEN u.avg_historical_kwh > 0 
         THEN (u.avg_recent_kwh - u.avg_historical_kwh) / u.avg_historical_kwh * 100 
         ELSE NULL END, 1
  ) as recent_vs_historical_pct,
  
  -- City benchmark comparisons
  cb.city_avg_lifetime_kwh,
  cb.city_median_lifetime_kwh,
  ROUND((u.lifetime_kwh - cb.city_avg_lifetime_kwh) / NULLIF(cb.city_avg_lifetime_kwh, 0) * 100, 1) as vs_city_avg_pct,
  
  -- Complex customer segmentation
  CASE 
    WHEN u.lifetime_kwh > cb.city_median_lifetime_kwh * 2 THEN 'High Value'
    WHEN u.lifetime_kwh > cb.city_median_lifetime_kwh * 1.5 THEN 'Above Average Value'
    WHEN u.lifetime_kwh > cb.city_median_lifetime_kwh * 0.5 THEN 'Average Value'
    WHEN u.lifetime_kwh > cb.city_median_lifetime_kwh * 0.25 THEN 'Below Average Value'
    ELSE 'Low Value'
  END as customer_value_segment,
  
  -- Technology adoption patterns
  CASE 
    WHEN c.has_solar AND c.has_ev AND c.has_battery_storage THEN 'Full Tech Adopter'
    WHEN c.has_solar AND c.has_ev THEN 'Solar + EV'
    WHEN c.has_solar AND c.has_battery_storage THEN 'Solar + Storage'
    WHEN c.has_ev AND c.has_battery_storage THEN 'EV + Storage'
    WHEN c.has_solar THEN 'Solar Only'
    WHEN c.has_ev THEN 'EV Only'
    WHEN c.has_battery_storage THEN 'Storage Only'
    ELSE 'No Tech'
  END as technology_profile,
  
  -- Risk and opportunity flags
  CASE 
    WHEN u.total_usage_days < 100 AND DATE_DIFF(CURRENT_DATE(), u.last_usage_date, DAY) > 30 THEN 'Churn Risk'
    WHEN COALESCE(p.programs_enrolled, 0) = 0 AND u.lifetime_kwh > cb.city_median_lifetime_kwh THEN 'Program Opportunity'
    WHEN c.is_low_income_qualified AND COALESCE(p.assistance_programs, 0) = 0 THEN 'Assistance Opportunity'
    WHEN c.has_solar AND COALESCE(p.solar_programs, 0) = 0 THEN 'Solar Program Opportunity'
    WHEN c.has_ev AND COALESCE(p.ev_programs, 0) = 0 THEN 'EV Program Opportunity'
    WHEN u.usage_volatility_kwh > u.avg_daily_kwh * 3 THEN 'High Volatility'
    ELSE 'Stable'
  END as customer_flag

FROM `cca-bigquery-analytics.cca_demo.customers` c
JOIN customer_usage_summary u ON c.customer_id = u.customer_id
LEFT JOIN program_analysis p ON c.customer_id = p.customer_id
LEFT JOIN city_benchmarks cb ON c.city = cb.city AND c.customer_type = cb.customer_type
WHERE u.total_usage_days > 10  -- Filter out customers with minimal usage data
ORDER BY u.lifetime_kwh DESC;