
-- CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.latest_customer_records_gold_demo
-- AS
-- SELECT
--   customer_id, 
--   address, 
--   name, 
--   __START_AT, 
--   __END_AT,
--   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __START_AT DESC) AS latest_record -- Get the latest record of each customer
-- FROM sdp_cdc_2_silver.customers_silver_scd2_demo
-- QUALIFY latest_record = 1;

-- CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.removed_customer_records_gold_demo
-- AS
-- SELECT
--   customer_id, 
--   address, 
--   name, 
--   __START_AT, 
--   __END_AT
-- FROM sdp_cdc_3_gold.latest_customer_records_gold_demo
-- WHERE __END_AT IS NOT NULL; 


-- b. Create Gold Materialized View for Removed Customers
-- CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.removed_customers_gold_demo
-- COMMENT "List of customers identified as removed or inactive"
-- AS
-- SELECT
--   * EXCEPT (processing_time),
--   current_timestamp() AS removed_at
-- FROM sdp_cdc_2_silver.customers_silver_scd2_demo
-- WHERE __END_AT IS NOT NULL;   -- Filter for records that contain an __END_AT value, indicating they have been removed