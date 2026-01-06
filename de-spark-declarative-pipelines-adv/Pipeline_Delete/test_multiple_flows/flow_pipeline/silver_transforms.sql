CREATE OR REFRESH STREAMING TABLE multi_flows_2_silver.orders_silver_flows_demo
(
  -- Typed schema for silver to avoid schema evolution (Optional)
  subsidiary_id   STRING,
  order_id        STRING,
  order_timestamp TIMESTAMP,
  order_date      DATE,
  customer_id     STRING,
  region          STRING,
  country         STRING,
  city            STRING,
  channel         STRING,
  sku             STRING,
  category        STRING,
  qty             INT,
  unit_price      DOUBLE,
  discount_pct    DOUBLE,
  total_amount    DOUBLE,
  coupon_code     STRING

  -- Data quality expectations (constraints) on the columns
  CONSTRAINT qty_valid          EXPECT (qty >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT total_amount_valid EXPECT (total_amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_not_null EXPECT (order_timestamp IS NOT NULL)
)
COMMENT 'Clean the multiple flow bronze streaming table for all subsidiaries'
-- Add liquid clustering to the silver table by these columns
CLUSTER BY (order_date, subsidiary_id)
AS
SELECT
  subsidiary_id,
  order_id,
  TRY_CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
  TRY_CAST(order_date      AS DATE)      AS order_date,
  customer_id,
  region,
  country,
  city,
  channel,
  sku,
  category,
  TRY_CAST(qty          AS INT)    AS qty,
  TRY_CAST(unit_price   AS DOUBLE) AS unit_price,
  TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
  TRY_CAST(total_amount AS DOUBLE) AS total_amount,
  coupon_code
FROM STREAM multi_flows_1_bronze.orders_bronze_flows_demo;
