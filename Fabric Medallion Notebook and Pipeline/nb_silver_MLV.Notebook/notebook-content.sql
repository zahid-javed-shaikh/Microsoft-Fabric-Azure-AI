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
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "548dd697-39e6-b0b5-4eab-951ae3694047",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "548dd697-39e6-b0b5-4eab-951ae3694047",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

# ══════════════════════════════════════════════
# CELL 1 — VERIFY BOTH SOURCES ARE VISIBLE
# ══════════════════════════════════════════════
spark.sql("SHOW DATABASES").show(truncate=False)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════════
-- CELL 1 — silver.mlv_customers
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_customers
AS
SELECT
    CAST(c.customer_id AS BIGINT)           AS customer_key,
    TRIM(c.name)                            AS customer_name,
    LOWER(TRIM(c.email))                    AS email,
    TRIM(c.phone)                           AS phone,
    TRIM(a.address)                         AS address,
    TRIM(a.city)                            AS city,
    TRIM(a.state)                           AS state,
    TRIM(a.pincode)                         AS pincode,
    c.created_at                            AS customer_since
FROM dbo.bronze_customers c
LEFT JOIN (
    SELECT customer_id, address, city, state, pincode
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id DESC) AS rn
        FROM dbo.bronze_customer_addresses
    ) t WHERE rn = 1
) a ON c.customer_id = a.customer_id
WHERE c.deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;     SHOW SCHEMAS;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_customers
AS
SELECT
    CAST(c.id AS BIGINT)                                        AS customer_key,
    TRIM(c.name)                                                AS customer_name,
    LOWER(TRIM(c.email))                                        AS email,
    TRIM(c.direct_phone)                                        AS phone,
    TRIM(c.contact_no)                                          AS contact_no,
    TRIM(c.designation)                                         AS designation,
    TRIM(c.billing_cycle)                                       AS billing_cycle,
    COALESCE(c.credit_limit_amount, 0)                          AS credit_limit_amount,
    COALESCE(UPPER(TRIM(c.status)), 'UNKNOWN')                  AS status,
    -- most recent address
    TRIM(a.address_line_1)                                      AS address_line_1,
    TRIM(a.address_line_2)                                      AS address_line_2,
    TRIM(a.pincode)                                             AS pincode,
    a.city_id,
    a.state_id,
    c.created_at                                                AS customer_since
FROM dbo.bronze_customers c
LEFT JOIN (
    SELECT customer_id, address_line_1, address_line_2, pincode, city_id, state_id
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id DESC) AS rn
        FROM dbo.bronze_customer_addresses
        WHERE deleted_at IS NULL
    ) t WHERE rn = 1
) a ON c.id = a.customer_id
WHERE c.deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_consignors
AS
SELECT
    CAST(c.id AS BIGINT)                                        AS consignor_key,
    TRIM(c.name)                                                AS consignor_name,
    TRIM(c.invoice)                                             AS invoice_ref,
    CAST(c.customer_id AS BIGINT)                               AS customer_id,
    TRIM(a.address_line1)                                       AS address_line1,
    TRIM(a.pincode)                                             AS pincode,
    TRIM(a.area)                                                AS area,
    TRIM(a.location)                                            AS location,
    a.city_id,
    a.state_id,
    a.warehouse_id,
    c.created_at
FROM dbo.bronze_consignors c
LEFT JOIN (
    SELECT consignor_id, address_line1, pincode, area, location, city_id, state_id, warehouse_id
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY consignor_id ORDER BY id DESC) AS rn
        FROM dbo.bronze_consignor_addresses
    ) t WHERE rn = 1
) a ON c.id = a.consignor_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

 SELECT *,
               ROW_NUMBER() OVER (PARTITION BY consignor_id ORDER BY id DESC) AS rn
        FROM dbo.bronze_consignor_addresses

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SHOW TABLES IN silver;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_delivery_partners
AS
SELECT
    CAST(id AS BIGINT)                                          AS delivery_partner_key,
    TRIM(name)                                                  AS partner_name,
    created_at
FROM dbo.bronze_delivery_partners
WHERE deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_users
AS
SELECT
    CAST(id AS BIGINT)                                          AS user_key,
    TRIM(name)                                                  AS user_name,
    LOWER(TRIM(email))                                          AS email,
    is_active,
    created_at
FROM dbo.bronze_users;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_vendors
AS
SELECT
    CAST(id AS BIGINT)                                          AS vendor_key,
    TRIM(name)                                                  AS vendor_name,
    LOWER(TRIM(email))                                          AS email,
    TRIM(phone)                                                 AS phone,
    TRIM(address)                                               AS address,
    TRIM(company_name)                                          AS company_name,
    TRIM(tax_id)                                                AS tax_id,
    status                                                      AS is_active,
    created_at
FROM dbo.bronze_vendors;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_warehouses
AS
SELECT
    CAST(id AS BIGINT)                                          AS warehouse_key,
    TRIM(warehouse_name)                                        AS warehouse_name,
    TRIM(warehouse_code)                                        AS warehouse_code,
    TRIM(address_line_1)                                        AS address_line_1,
    TRIM(address_line_2)                                        AS address_line_2,
    TRIM(pin)                                                   AS pincode,
    city_id,
    state_id,
    is_active,
    is_dlv,
    created_at
FROM dbo.bronze_warehouses;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_cities
AS
SELECT
    CAST(id AS BIGINT)                                          AS city_key,
    TRIM(name)                                                  AS city_name,
    CAST(state_id AS BIGINT)                                    AS state_id
FROM dbo.bronze_cities
WHERE deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_states
AS
SELECT
    CAST(id AS BIGINT)                                          AS state_key,
    TRIM(name)                                                  AS state_name,
    TRIM(code)                                                  AS state_code
FROM dbo.bronze_states
WHERE deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_users
AS
SELECT
    CAST(id AS BIGINT)                                          AS user_key,
    TRIM(name)                                                  AS user_name,
    LOWER(TRIM(email))                                          AS email,
    is_active,
    created_at
FROM dbo.bronze_users;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_dockets
AS
SELECT
    -- keys
    CAST(d.id AS BIGINT)                                        AS docket_key,
    CAST(d.order_id AS BIGINT)                                  AS order_id,
    CAST(o.customer_id AS BIGINT)                               AS customer_key,
    CAST(o.consignor_id AS BIGINT)                              AS consignor_key,
    CAST(d.created_by AS BIGINT)                                AS user_key,

    -- date keys (YYYYMMDD integer for joining to dim_date)
    CAST(DATE_FORMAT(d.created_at, 'yyyyMMdd') AS INT)          AS created_date_key,
    CAST(DATE_FORMAT(d.invoice_date, 'yyyyMMdd') AS INT)        AS invoice_date_key,
    CAST(DATE_FORMAT(d.edd, 'yyyyMMdd') AS INT)                 AS edd_date_key,

    -- degenerate dimensions (descriptive — no separate dim table needed)
    TRIM(d.docket_number)                                       AS docket_number,
    TRIM(d.awb_number)                                          AS awb_number,
    TRIM(d.invoice_number)                                      AS invoice_number,
    TRIM(d.vendor_name)                                         AS vendor_name,
    COALESCE(UPPER(TRIM(d.shipment_status)), 'UNKNOWN')         AS shipment_status,
    COALESCE(UPPER(TRIM(d.payment_type)), 'UNKNOWN')            AS payment_type,
    d.transport_mode,
    TRIM(d.delivery_partner)                                    AS delivery_partner_name,
    TRIM(d.consignee_name)                                      AS consignee_name,
    TRIM(d.address_line1)                                       AS delivery_address_line1,
    TRIM(d.address_line2)                                       AS delivery_address_line2,
    TRIM(d.city)                                                AS delivery_city,
    TRIM(d.state)                                               AS delivery_state,
    TRIM(d.pincode)                                             AS delivery_pincode,
    TRIM(d.mobile)                                              AS consignee_mobile,
    TRIM(d.item_description)                                    AS item_description,
    d.is_fragile,
    d.self_shipment,

    -- measures (additive — sum these in Power BI)
    COALESCE(d.collectable_value, 0)                            AS collectable_value,
    COALESCE(d.actual_weight, 0)                                AS actual_weight_kg,
    COALESCE(d.adj_wtg, 0)                                      AS adjusted_weight_kg,
    COALESCE(d.volumetric_weight, 0)                            AS volumetric_weight_kg,
    COALESCE(d.length, 0)                                       AS length_cm,
    COALESCE(d.breadth, 0)                                      AS breadth_cm,
    COALESCE(d.height, 0)                                       AS height_cm,

    -- from orders header
    TRIM(o.order_number)                                        AS order_number,
    COALESCE(o.total_value, 0)                                  AS order_total_value,
    o.is_partial_support,

    d.created_at,
    d.updated_at

FROM dbo.bronze_dockets d
LEFT JOIN dbo.bronze_orders o
    ON d.order_id = o.id
    AND o.deleted_at IS NULL
WHERE d.deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.mlv_pickups
AS
SELECT
    CAST(id AS BIGINT)                                          AS pickup_key,
    TRIM(pickup_request_number)                                 AS pickup_request_number,
    pickup_date,
    TRIM(pickup_time)                                           AS pickup_time,
    TRIM(delivery_partner)                                      AS delivery_partner_name,
    COALESCE(UPPER(TRIM(status)), 'UNKNOWN')                    AS pickup_status,

    -- measures
    COALESCE(total_dockets, 0)                                  AS total_dockets,
    COALESCE(processed_dockets, 0)                              AS processed_dockets,
    COALESCE(failed_dockets, 0)                                 AS failed_dockets,
    -- derived: success rate
    CASE
        WHEN COALESCE(total_dockets, 0) = 0 THEN 0
        ELSE ROUND(processed_dockets / total_dockets * 100, 2)
    END                                                         AS processing_success_pct,

    CAST(DATE_FORMAT(created_at, 'yyyyMMdd') AS INT)            AS created_date_key,
    CAST(DATE_FORMAT(pickup_date, 'yyyyMMdd') AS INT)           AS pickup_date_key,
    CAST(created_by AS BIGINT)                                  AS user_key,
    created_at
FROM dbo.bronze_pickup_requests
WHERE deleted_at IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM LH_Logibuddy.silver.mlv_dockets LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM LH_Logibuddy.silver.mlv_consignors LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM LH_Logibuddy.silver.mlv_customers LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM LH_Logibuddy.dbo.bronze_customer_addresses LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE dbo.bronze_customers;
DESCRIBE dbo.bronze_customer_addresses;
DESCRIBE dbo.bronze_consignors;
DESCRIBE dbo.bronze_consignor_addresses;
DESCRIBE dbo.bronze_financial_information;
DESCRIBE dbo.bronze_pickup_requests;



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }

-- CELL ********************

DESCRIBE dbo.bronze_dockets;  
DESCRIBE dbo.bronze_orders;
DESCRIBE dbo.bronze_vendors;
DESCRIBE dbo.bronze_warehouses;
DESCRIBE dbo.bronze_users;
DESCRIBE dbo.bronze_cities;
DESCRIBE dbo.bronze_states;
DESCRIBE dbo.bronze_delivery_partners;  

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }

-- CELL ********************

DESCRIBE dbo.bronze_cities;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ══════════════════════════════════════════════════════
-- CELL 1: DIM_DATE
-- Generates a date spine from 2020 to 2030
-- Used to slice all facts by day, month, quarter, year
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS WH_Logibuddy.dbo.dim_date AS
SELECT
    CAST(DATE_FORMAT(cal.dt, 'yyyyMMdd') AS INT)   AS date_key,       -- surrogate key e.g. 20240101
    cal.dt                                          AS full_date,
    DAY(cal.dt)                                     AS day,
    MONTH(cal.dt)                                   AS month,
    DATE_FORMAT(cal.dt, 'MMMM')                     AS month_name,
    QUARTER(cal.dt)                                 AS quarter,
    YEAR(cal.dt)                                    AS year,
    DATE_FORMAT(cal.dt, 'EEEE')                     AS day_name,
    CASE WHEN DAYOFWEEK(cal.dt) IN (1,7) 
         THEN TRUE ELSE FALSE END                   AS is_weekend
FROM (
    SELECT EXPLODE(
        SEQUENCE(
            TO_DATE('2020-01-01'),
            TO_DATE('2030-12-31'),
            INTERVAL 1 DAY
        )
    ) AS dt
) cal;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
