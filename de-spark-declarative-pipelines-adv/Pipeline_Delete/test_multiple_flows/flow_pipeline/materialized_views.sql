------------------------------------------
-- GOLD MATERIALIZED VIEW: DAILY SUBSIDIARY SCORECARD
-- High level daily summary showing revenue, units, and order count per subsidiary
------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW multi_flows_3_gold.mv_daily_subsidiary_scorecard_demo
AS
SELECT
  order_date,
  subsidiary_id,
  COUNT(DISTINCT order_id)    AS order_count,
  SUM(total_amount)           AS total_revenue,
  SUM(qty)                    AS total_units,
  AVG(total_amount)           AS avg_order_value
FROM multi_flows_2_silver.orders_silver_flows_demo
WHERE order_date IS NOT NULL
GROUP BY order_date, subsidiary_id;


------------------------------------------
-- GOLD MATERIALIZED VIEW: PRODUCT PERFORMANCE BY SUBSIDIARY
-- Useful for identifying which products/categories perform best per subsidiary
------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW multi_flows_3_gold.mv_product_performance_by_subsidiary_demo
AS
SELECT
  subsidiary_id,
  category,
  sku,
  SUM(qty)                   AS units_sold,
  SUM(total_amount)          AS revenue,
  COUNT(DISTINCT order_id)   AS order_count
FROM multi_flows_2_silver.orders_silver_flows_demo
GROUP BY subsidiary_id, category, sku;


------------------------------------------
-- GOLD MATERIALIZED VIEW: CHANNEL MIX BY REGION
-- Helps understand distribution of orders across channels within each region/country
------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW multi_flows_3_gold.mv_channel_mix_by_region
AS
SELECT
  region,
  country,
  channel,
  COUNT(DISTINCT order_id)   AS order_count,
  SUM(total_amount)          AS revenue,
  ROUND(
    100.0 * COUNT(DISTINCT order_id)
      / SUM(COUNT(DISTINCT order_id)) OVER (PARTITION BY region, country),
    2
  ) AS order_pct_in_region
FROM multi_flows_2_silver.orders_silver_flows_demo
GROUP BY region, country, channel;
