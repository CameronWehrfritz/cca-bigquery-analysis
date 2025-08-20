# CCA BigQuery Analytics - Data Dictionary

## Overview
This data dictionary documents the complete data model for the CCA (Community Choice Aggregation) analytics dataset. The database follows proper relational design principles with declared primary and foreign key relationships documented for data integrity governance and optimal query performance.

**Database:** `cca-bigquery-analytics.cca_demo`  
**Total Tables:** 4  
**Total Records:** ~39.6M across all tables  
**Update Frequency:** Daily for usage facts, as-needed for dimensions

---

## Table Relationships

```
customers (1) ←─── (M) daily_usage_facts
    │
    └─── (M) program_enrollments

data_summary (metadata table)
```

**Primary Relationships:**
- `daily_usage_facts.customer_id` → `customers.customer_id` (declared FK)
- `program_enrollments.customer_id` → `customers.customer_id` (declared FK)

---

## Table 1: customers

**Purpose:** Master customer dimension table containing demographic and enrollment information.

**Primary Key:** `customer_id` (declared, not enforced)

| Column Name | Data Type | Nullable | Description | Business Rules | Example Values |
|-------------|-----------|----------|-------------|----------------|----------------|
| `customer_id` | STRING | NO | Unique identifier for each customer account | Format: CUST_XXXXXX (6-digit numeric suffix), Declared as Primary Key | CUST_000026, CUST_031045 |
| `customer_type` | STRING | YES | Customer account classification | Values: 'Residential', 'Small Commercial', 'Large Commercial' | Residential, Small Commercial |
| `city` | STRING | YES | Customer service address city | Limited to CCA service territory | San Mateo, Redwood City, Palo Alto |
| `rate_plan` | STRING | YES | Customer's enrolled rate schedule | ECO (basic), ECO100 (100% renewable), ECOplus (premium) | ECO, ECO100, ECOplus |
| `is_low_income_qualified` | BOOL | YES | Low-income assistance program eligibility | Based on income verification | true, false |
| `has_solar` | BOOL | YES | Solar panel installation status | Self-reported or interconnection data | true, false |
| `has_ev` | BOOL | YES | Electric vehicle ownership | Self-reported during enrollment | true, false |
| `has_battery_storage` | BOOL | YES | Battery storage system installation | Self-reported or interconnection data | true, false |
| `enrollment_date` | DATE | YES | Date of first CCA enrollment | Format: YYYY-MM-DD, cannot be future date | 2022-09-07, 2023-12-06 |

**Data Quality Notes:**
- `customer_id` declared as primary key for documentation purposes (BigQuery does not enforce uniqueness constraints)
- Geographic coverage limited to 8 cities in service territory
- Boolean fields use consistent true/false values
- Enrollment dates range from 2020-01-01 to 2023-12-31

---

## Table 2: daily_usage_facts

**Purpose:** Daily energy usage fact table with consumption, cost, and contextual data.

**Primary Key:** `(usage_date, customer_id)` (Composite, declared)  
**Foreign Key:** `customer_id` → `customers.customer_id` (declared) 
**Partitioning:** `usage_date` (daily partitions)  
**Clustering:** `customer_type`, `city`

| Column Name | Data Type | Nullable | Description | Business Rules | Example Values |
|-------------|-----------|----------|-------------|----------------|----------------|
| `usage_date` | DATE | NO | Date of energy usage measurement | Part of declared composite primary key | 2024-08-01, 2022-01-15 |
| `customer_id` | STRING | NO | Customer account identifier | Declared foreign key to customers table, part of composite PK | CUST_000026, CUST_031045 |
| `kwh_used` | FLOAT64 | YES | Daily energy consumption in kilowatt-hours | Must be >= 0, typically 1-2000 kWh | 25.6, 847.3, 1205.7 |
| `peak_demand_kw` | FLOAT64 | YES | Peak power demand in kilowatts | Calculated as percentage of total usage | 1.2, 45.7, 89.2 |
| `cost_dollars` | FLOAT64 | YES | Daily energy cost in USD | Calculated from usage * rate plan pricing | 5.12, 165.23, 248.07 |
| `rate_plan` | STRING | YES | Rate plan (denormalized from customers) | Same values as customers.rate_plan | ECO, ECO100, ECOplus |
| `customer_type` | STRING | YES | Customer type (denormalized) | Same values as customers.customer_type | Residential, Small Commercial |
| `city` | STRING | YES | Customer city (denormalized) | Same values as customers.city | San Mateo, Burlingame |
| `temperature_high_f` | INT64 | YES | Daily high temperature in Fahrenheit | Synthetic weather data, typically 40-90°F | 75, 62, 85 |
| `is_weekend` | BOOL | YES | Whether date falls on weekend | Saturday (7) or Sunday (1) = true | true, false |
| `is_holiday` | BOOL | YES | Whether date is a recognized holiday | Major US holidays only | true, false |

**Data Quality Notes:**
- Composite primary key documented for intended uniqueness (usage_date, customer_id) - maintained by ETL process
- ~39.6M total records spanning 2022-01-01 to 2024-08-31
- Denormalized customer attributes for query performance
- Date range limited to customer enrollment date forward

---

## Table 3: program_enrollments

**Purpose:** Customer participation in CCA programs and incentive tracking.

**Primary Key:** `(customer_id, program_name, program_enrollment_date)` (Composite, declared)  
**Foreign Key:** `customer_id` → `customers.customer_id` (declared)

| Column Name | Data Type | Nullable | Description | Business Rules | Example Values |
|-------------|-----------|----------|-------------|----------------|----------------|
| `customer_id` | STRING | NO | Customer account identifier | Declared foreign key to customers, part of composite PK | CUST_000026, CUST_031045 |
| `program_name` | STRING | NO | Name of CCA program | Part of composite primary key | Solar Rebate, EV Charging Rebate |
| `program_category` | STRING | YES | Program classification category | Groups related programs | Distributed Energy Resources, Transportation Electrification |
| `program_enrollment_date` | DATE | NO | Date customer enrolled in program | Part of composite PK, should occur after customer enrollment date | 2023-03-15, 2022-11-08 |
| `incentive_amount` | FLOAT64 | YES | Financial incentive provided (USD) | Varies by program type | 2500.00, 750.00, 4200.00 |
| `program_status` | STRING | YES | Current status of participation | Active, Completed, Pending | Active, Completed |
| `program_description` | STRING | YES | Detailed program description | Human-readable program details | Solar panel installation incentive |

**Program Categories:**
- **Distributed Energy Resources**: Solar Rebate, Battery Storage
- **Transportation Electrification**: EV Charging Rebate  
- **Demand Management**: Energy Efficiency
- **Community Benefits**: Energy Assistance
- **Grid Resilience**: Battery Storage

**Data Quality Notes:**
- ~32K total program enrollments
- Enrollment date business rule: should occur after customer's CCA enrollment (enforced by ETL process, not database constraints)
- Multiple program participation allowed per customer
- Incentive amounts vary by program type ($200-$8000 range)

---

## Table 4: data_summary

**Purpose:** Metadata table providing data quality metrics and table statistics.

**Primary Key:** None (summary/reporting table)

| Column Name | Data Type | Nullable | Description | Business Rules | Example Values |
|-------------|-----------|----------|-------------|----------------|----------------|
| `table_name` | STRING | YES | Name of source table | References tables in this database | customers, daily_usage_facts |
| `record_count` | INT64 | YES | Total number of records | Current count at time of generation | 50000, 39616738 |
| `min_date` | DATE | YES | Earliest date in date-related columns | Depends on table structure | 2020-01-01, 2022-01-01 |
| `max_date` | DATE | YES | Latest date in date-related columns | Depends on table structure | 2023-12-31, 2024-08-31 |
| `description` | STRING | YES | Human-readable table description | Business context for each table | Customer master data |

**Current Statistics:**
- `customers`: 50,000 records (2020-2023 enrollment dates)
- `daily_usage_facts`: 39,616,738 records (2022-2024 usage dates)  
- `program_enrollments`: 32,121 records (2020-2024 program dates)

---

## Query Optimization Guidelines

**For daily_usage_facts (large table):**
- Always filter by `usage_date` to leverage partitioning
- Use `customer_type` and `city` in WHERE clauses when possible (clustering)
- Avoid SELECT * on this table (39.6M records)

**For JOINs:**
- Use `customer_id` for all customer-related joins
- Prefer EXISTS over IN for large subqueries
- Consider date range filtering before joins

**Example Optimized Query:**
```sql
SELECT c.customer_type, AVG(d.kwh_used) as avg_usage_kwh
FROM `cca-bigquery-analytics.cca_demo.customers` c
JOIN `cca-bigquery-analytics.cca_demo.daily_usage_facts` d
  ON c.customer_id = d.customer_id
WHERE d.usage_date >= '2024-01-01'  -- Partition filter
  AND d.customer_type = 'Residential'  -- Cluster filter
GROUP BY c.customer_type;
```

---

## Common Use Cases

**Customer Analysis:**
- Segmentation by technology adoption (solar, EV, battery)
- Geographic performance analysis by city
- Rate plan effectiveness measurement

**Usage Analytics:**
- Seasonal pattern analysis with temperature correlation
- Weekend vs weekday consumption patterns
- Holiday impact assessment

**Program Effectiveness:**
- Before/after usage analysis for program participants
- ROI calculation for incentive programs
- Participation rate analysis by customer segment

**Operational Reporting:**
- Growth trend analysis and forecasting
- Data quality monitoring and validation
- Regulatory compliance reporting

---

*Last Updated: 2025-08-20*  
*Created by: Cameron Wehrfritz*  
*Purpose: Technical documentation for CCA analytics database supporting utility operations and regulatory compliance*