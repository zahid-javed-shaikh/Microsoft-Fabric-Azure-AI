-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "6aca0269-5052-49af-b2c5-635f48ffb667",
-- META       "default_lakehouse_name": "LH_Logibuddy",
-- META       "default_lakehouse_workspace_id": "2150bb83-47b4-4fde-a244-6be5b7e62908",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "6aca0269-5052-49af-b2c5-635f48ffb667"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SHOW TABLES IN silver;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_DATE — full date spine 2020–2030
-- Every fact joins to this on date_key (integer YYYYMMDD)
-- No source table needed — generated from sequence
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_date
AS
SELECT
    CAST(DATE_FORMAT(dt, 'yyyyMMdd') AS INT)        AS date_key,
    dt                                              AS full_date,
    DAY(dt)                                         AS day_of_month,
    MONTH(dt)                                       AS month_number,
    DATE_FORMAT(dt, 'MMMM')                         AS month_name,
    DATE_FORMAT(dt, 'MMM')                          AS month_short,
    QUARTER(dt)                                     AS quarter_number,
    CONCAT('Q', QUARTER(dt))                        AS quarter_label,
    YEAR(dt)                                        AS year,
    CONCAT('Q', QUARTER(dt), ' ', YEAR(dt))         AS quarter_year,
    DAYOFWEEK(dt)                                   AS day_of_week,
    DATE_FORMAT(dt, 'EEEE')                         AS day_name,
    DATE_FORMAT(dt, 'EEE')                          AS day_short,
    CASE WHEN DAYOFWEEK(dt) IN (1,7)
         THEN TRUE ELSE FALSE END                   AS is_weekend,
    CASE WHEN DAYOFWEEK(dt) NOT IN (1,7)
         THEN TRUE ELSE FALSE END                   AS is_weekday,
    -- useful for Power BI relative date filtering
    DATEDIFF(dt, CURRENT_DATE())                    AS days_from_today,
    CASE WHEN dt <= CURRENT_DATE()
         THEN TRUE ELSE FALSE END                   AS is_past_or_today
FROM (
    SELECT EXPLODE(
        SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)
    ) AS dt
) dates;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_CUSTOMERS — enriched with city and state names
-- Joins silver customers → silver cities → silver states
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_customers
AS
SELECT
    c.customer_key,
    c.customer_name,
    c.email,
    c.phone,
    c.contact_no,
    c.designation,
    c.billing_cycle,
    c.credit_limit_amount,
    c.status,
    c.address_line_1,
    c.address_line_2,
    c.pincode,
    ci.city_name,
    s.state_name,
    s.state_code,
    c.customer_since,
    -- segment by credit limit for Power BI slicing
    CASE
        WHEN c.credit_limit_amount >= 500000  THEN 'Platinum'
        WHEN c.credit_limit_amount >= 100000  THEN 'Gold'
        WHEN c.credit_limit_amount >= 50000   THEN 'Silver'
        ELSE 'Standard'
    END                                                 AS customer_segment
FROM silver.mlv_customers c
LEFT JOIN silver.mlv_cities  ci ON c.city_id  = ci.city_key
LEFT JOIN silver.mlv_states  s  ON c.state_id = s.state_key;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_CONSIGNORS — enriched with city and state names
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_consignors
AS
SELECT
    c.consignor_key,
    c.consignor_name,
    c.invoice_ref,
    c.address_line1,
    c.pincode,
    c.area,
    c.location                                          AS landmark,
    ci.city_name,
    s.state_name,
    s.state_code,
    c.warehouse_id,
    c.created_at
FROM silver.mlv_consignors c
LEFT JOIN silver.mlv_cities  ci ON c.city_id  = ci.city_key
LEFT JOIN silver.mlv_states  s  ON c.state_id = s.state_key;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_DELIVERY_PARTNERS — courier companies
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_delivery_partners
AS
SELECT
    delivery_partner_key,
    partner_name,
    created_at
FROM silver.mlv_delivery_partners;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_VENDORS — third party vendors
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_vendors
AS
SELECT
    vendor_key,
    vendor_name,
    email,
    phone,
    address,
    company_name,
    tax_id,
    is_active,
    created_at
FROM silver.mlv_vendors;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_WAREHOUSES — physical hubs enriched with city/state
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_warehouses
AS
SELECT
    w.warehouse_key,
    w.warehouse_name,
    w.warehouse_code,
    w.address_line_1,
    w.address_line_2,
    w.pincode,
    ci.city_name,
    s.state_name,
    s.state_code,
    w.is_active,
    w.is_dlv,
    w.created_at
FROM silver.mlv_warehouses w
LEFT JOIN silver.mlv_cities  ci ON w.city_id  = ci.city_key
LEFT JOIN silver.mlv_states  s  ON w.state_id = s.state_key;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- DIM_USERS — internal staff who created dockets/pickups
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_users
AS
SELECT
    user_key,
    user_name,
    email,
    is_active,
    created_at
FROM silver.mlv_users;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- FACT_DOCKETS — grain: one row per shipment
-- Central fact — connects to all dims via foreign keys
-- Contains all additive measures for Power BI aggregation
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_dockets
AS
SELECT
    -- surrogate keys
    docket_key,
    order_id,

    -- foreign keys to dims
    customer_key,
    consignor_key,
    user_key,

    -- date foreign keys → join to gold.dim_date
    created_date_key,
    invoice_date_key,
    edd_date_key,

    -- degenerate dimensions (stay in fact — no dim table needed)
    docket_number,
    awb_number,
    invoice_number,
    vendor_name,
    shipment_status,
    payment_type,
    transport_mode,
    delivery_partner_name,
    consignee_name,
    delivery_city,
    delivery_state,
    delivery_pincode,
    item_description,
    is_fragile,
    self_shipment,
    order_number,
    is_partial_support,

    -- ── ADDITIVE MEASURES ─────────────────────────────
    collectable_value,
    actual_weight_kg,
    adjusted_weight_kg,
    volumetric_weight_kg,
    order_total_value,
    length_cm,
    breadth_cm,
    height_cm,

    -- derived measures — calculated once here, reused in Power BI
    ROUND(length_cm * breadth_cm * height_cm / 5000, 2)        AS dim_weight_kg,
    GREATEST(actual_weight_kg, volumetric_weight_kg)            AS chargeable_weight_kg,

    created_at,
    updated_at

FROM silver.mlv_dockets;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- FACT_PICKUPS — grain: one row per pickup request
-- Measures: docket counts and processing success rate
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_pickups
AS
SELECT
    pickup_key,
    user_key,
    created_date_key,
    pickup_date_key,
    pickup_request_number,
    pickup_date,
    delivery_partner_name,
    pickup_status,

    -- MEASURES
    total_dockets,
    processed_dockets,
    failed_dockets,
    processing_success_pct,

    -- derived
    total_dockets - processed_dockets                           AS pending_dockets,

    created_at
FROM silver.mlv_pickups;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- AGG_DOCKETS_DAILY — pre-aggregated daily summary
-- Speeds up Power BI dashboards on large datasets
-- Use this for trend charts instead of fact_dockets
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.agg_dockets_daily
AS
SELECT
    created_date_key,
    delivery_city,
    delivery_state,
    shipment_status,
    payment_type,
    delivery_partner_name,

    -- aggregated measures
    COUNT(*)                                                    AS total_dockets,
    SUM(collectable_value)                                      AS total_collectable_value,
    SUM(order_total_value)                                      AS total_order_value,
    SUM(actual_weight_kg)                                       AS total_actual_weight,
    SUM(chargeable_weight_kg)                                   AS total_chargeable_weight,
    AVG(collectable_value)                                      AS avg_collectable_value,
    AVG(chargeable_weight_kg)                                   AS avg_chargeable_weight,
    COUNT(DISTINCT customer_key)                                AS unique_customers,
    COUNT(DISTINCT consignor_key)                               AS unique_consignors,
    SUM(CASE WHEN is_fragile = TRUE THEN 1 ELSE 0 END)          AS fragile_dockets,
    SUM(CASE WHEN self_shipment = TRUE THEN 1 ELSE 0 END)       AS self_shipment_dockets

FROM gold.fact_dockets
GROUP BY
    created_date_key,
    delivery_city,
    delivery_state,
    shipment_status,
    payment_type,
    delivery_partner_name;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- AGG_CUSTOMER_SUMMARY — one row per customer
-- Lifetime value, order count, average order value
-- Perfect for customer analysis in Power BI
-- ══════════════════════════════════════════════════════

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.agg_customer_summary
AS
SELECT
    d.customer_key,
    c.customer_name,
    c.customer_segment,
    c.city_name,
    c.state_name,
    c.billing_cycle,
    c.credit_limit_amount,

    -- lifetime aggregates
    COUNT(DISTINCT d.docket_key)                                AS total_dockets,
    COUNT(DISTINCT d.order_id)                                  AS total_orders,
    SUM(d.collectable_value)                                    AS lifetime_collectable_value,
    SUM(d.order_total_value)                                    AS lifetime_order_value,
    AVG(d.order_total_value)                                    AS avg_order_value,
    SUM(d.actual_weight_kg)                                     AS total_weight_shipped_kg,
    MIN(d.created_at)                                           AS first_order_date,
    MAX(d.created_at)                                           AS last_order_date,
    DATEDIFF(CURRENT_DATE(), MAX(d.created_at))                 AS days_since_last_order,

    -- recency flag for Power BI
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), MAX(d.created_at)) <= 30  THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), MAX(d.created_at)) <= 90  THEN 'At Risk'
        ELSE 'Lapsed'
    END                                                         AS customer_recency_status

FROM gold.fact_dockets d
LEFT JOIN gold.dim_customers c ON d.customer_key = c.customer_key
GROUP BY
    d.customer_key,
    c.customer_name,
    c.customer_segment,
    c.city_name,
    c.state_name,
    c.billing_cycle,
    c.credit_limit_amount;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SHOW TABLES IN gold;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
