-- Stable, narrowly grantable interfaces; preserve exact numeric values for validation.
CREATE SCHEMA delivery AUTHORIZATION warehouse_owner;
CREATE VIEW delivery.invoice_details_v1 AS
 SELECT invoice_line_id,invoice_id,order_id,customer_id,customer_name,
 bill_to_customer_id,bill_to_customer_name,invoice_date,is_credit_note,
 stock_item_id,description,quantity,unit_price,tax_rate,tax_amount,line_profit,extended_price
 FROM mart.invoice_details;
CREATE VIEW delivery.order_details_v1 AS
 SELECT order_line_id,order_id,customer_id,customer_name,order_date,
 stock_item_id,description,quantity,unit_price,tax_rate,picked_quantity
 FROM mart.order_details;
CREATE VIEW delivery.ingestion_context AS
 SELECT s.consumer_version,s.verified_batches,s.new_events,s.last_batch_at,c.specification,
 false AS source_transaction_boundary_certified,false AS purge_authorized
 FROM ops.consumer_load_summary s JOIN ops.consumer_contracts c USING(consumer_version)
 WHERE s.consumer_version='sales/v2';
GRANT USAGE ON SCHEMA delivery TO warehouse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA delivery TO warehouse_reader;
