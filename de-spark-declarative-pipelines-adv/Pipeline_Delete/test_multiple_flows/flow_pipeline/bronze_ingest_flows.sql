------------------------------------------
-- CREATE THE BRONZE TABLE STRUCTURE
------------------------------------------
CREATE OR REPLACE STREAMING TABLE multi_flows_1_bronze.orders_bronze_flows_demo
(
  subsidiary_id   STRING,
  order_id        STRING,
  order_timestamp STRING,
  customer_id     STRING,
  region          STRING,
  country         STRING,
  city            STRING,
  channel         STRING,
  sku             STRING,
  category        STRING,
  qty             STRING,
  unit_price      STRING,
  discount_pct    STRING,
  coupon_code     STRING,
  total_amount    STRING,
  order_date      STRING,
  source_file     STRING,   -- Added by the _metadata column to return the source file name
  file_mod_time   TIMESTAMP -- Added by the _metadata column to return file modification time of the file. Returns a consistent value
)
COMMENT "Creates a single bronze streaming table with orders from all subsidiaries using multiple flows."
TBLPROPERTIES (
  'quality' = 'bronze'
);



------------------------------------------
-- BRONZE FLOW - BRIGHT HOME
------------------------------------------
-- Read CSV files from the bright_home_orders volume
CREATE FLOW bright_home_orders_flow
AS INSERT INTO multi_flows_1_bronze.orders_bronze_flows_demo BY NAME
SELECT
  CAST(subsidiary_id AS STRING) AS subsidiary_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(order_timestamp AS STRING) AS order_timestamp,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(region AS STRING) AS region,
  CAST(country AS STRING) AS country,
  CAST(city AS STRING) AS city,
  CAST(channel AS STRING) AS channel,
  CAST(sku AS STRING) AS sku,
  CAST(category AS STRING) AS category,
  CAST(qty AS STRING) AS qty,
  CAST(unit_price AS STRING) AS unit_price,
  CAST(discount_pct AS STRING) AS discount_pct,
  CAST(coupon_code AS STRING) AS coupon_code,
  CAST(total_amount AS STRING) AS total_amount,
  CAST(order_date AS STRING) AS order_date,
  CAST(_metadata.file_name AS STRING) AS source_file,
  _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
  '${bright_home_orders_path}',
  format => 'csv',
  header => true
);


------------------------------------------
-- BRONZE FLOW - LUMINA SPORTS
------------------------------------------
-- Read CSV files from the lumina_sports_orders volume
CREATE FLOW lumina_sports_orders_flow
AS INSERT INTO multi_flows_1_bronze.orders_bronze_flows_demo BY NAME
SELECT
  CAST(subsidiary_id AS STRING) AS subsidiary_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(order_timestamp AS STRING) AS order_timestamp,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(region AS STRING) AS region,
  CAST(country AS STRING) AS country,
  CAST(city AS STRING) AS city,
  CAST(channel AS STRING) AS channel,
  CAST(sku AS STRING) AS sku,
  CAST(category AS STRING) AS category,
  CAST(qty AS STRING) AS qty,
  CAST(unit_price AS STRING) AS unit_price,
  CAST(discount_pct AS STRING) AS discount_pct,
  CAST(coupon_code AS STRING) AS coupon_code,
  CAST(total_amount AS STRING) AS total_amount,
  CAST(order_date AS STRING) AS order_date,
  CAST(_metadata.file_name AS STRING) AS source_file,
  _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
  '${lumina_sports_orders_path}',
  format => 'csv',
  header => true
);



------------------------------------------
-- BRONZE FLOW - NORTHSTAR OUTFITTERS
------------------------------------------
-- Read JSON files from the northstar_outfitters_orders volume
CREATE FLOW northstar_outfitters_orders_flow
AS INSERT INTO multi_flows_1_bronze.orders_bronze_flows_demo BY NAME
SELECT
  CAST(subsidiary_id AS STRING) AS subsidiary_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(order_timestamp AS STRING) AS order_timestamp,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(region AS STRING) AS region,
  CAST(country AS STRING) AS country,
  CAST(city AS STRING) AS city,
  CAST(channel AS STRING) AS channel,
  CAST(sku AS STRING) AS sku,
  CAST(category AS STRING) AS category,
  CAST(qty AS STRING) AS qty,
  CAST(unit_price AS STRING) AS unit_price,
  CAST(discount_pct AS STRING) AS discount_pct,
  CAST(coupon_code AS STRING) AS coupon_code,
  CAST(total_amount AS STRING) AS total_amount,
  CAST(order_date AS STRING) AS order_date,
  CAST(_metadata.file_name AS STRING) AS source_file,
  _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
  '${northstar_outfitters_order_path}',
  format => 'json'
);