-- ================================================================
-- CCA BigQuery Analytics: Populate Daily Usage Facts
-- Generates realistic synthetic usage data for all customers
-- Full scale data load (39M+ records)
-- File: data_setup/03_populate_usage_facts.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-19
-- ================================================================

INSERT INTO `cca-bigquery-analytics.cca_demo.daily_usage_facts`
SELECT 
  usage_date,
  customer_id,
  kwh_used,
  
  -- Calculate peak demand based on usage
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
    
    -- Calculate realistic kwh_used based on customer type and external factors
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
    -- Generate all date-customer combinations
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