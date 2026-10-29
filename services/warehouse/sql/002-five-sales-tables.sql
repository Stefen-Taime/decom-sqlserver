-- Additive scope extension. Keep the original batch ledger and all event identities.
ALTER TABLE staging.cdc_event DROP CONSTRAINT cdc_event_table_name_check;
ALTER TABLE staging.cdc_event ADD CONSTRAINT cdc_event_table_name_check
 CHECK(table_name IN ('Sales.Customers','Sales.Orders','Sales.OrderLines','Sales.Invoices','Sales.InvoiceLines'));
ALTER TABLE staging.cdc_event ALTER COLUMN customer_hk DROP NOT NULL;
ALTER TABLE staging.cdc_event ADD COLUMN order_hk text, ADD COLUMN invoice_hk text,
 ADD COLUMN bill_to_customer_hk text, ADD COLUMN bill_to_link_hk text, ADD COLUMN order_link_hk text;
ALTER TABLE staging.cdc_event ADD CONSTRAINT cdc_event_customer_required
 CHECK(table_name NOT IN ('Sales.Customers','Sales.Orders','Sales.Invoices') OR customer_hk IS NOT NULL);

CREATE TABLE ops.consumer_batches (
 consumer_version text NOT NULL, batch_id text NOT NULL REFERENCES ops.archive_batches(batch_id),
 total_records integer NOT NULL, selected_records integer NOT NULL, new_events integer NOT NULL,
 ignored_records integer NOT NULL, completed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(consumer_version,batch_id),
 CHECK(total_records=selected_records+ignored_records), CHECK(new_events>=0 AND new_events<=selected_records)
);
CREATE INDEX consumer_batches_batch_idx ON ops.consumer_batches(batch_id);
INSERT INTO ops.consumer_batches SELECT consumer_version,batch_id,total_records,selected_records,new_events,ignored_records,completed_at
 FROM ops.archive_batches;
CREATE TABLE ops.consumer_contracts (consumer_version text PRIMARY KEY, specification jsonb NOT NULL);
INSERT INTO ops.consumer_contracts VALUES ('sales/v2',
 '{"tables":["Sales.Customers","Sales.InvoiceLines","Sales.Invoices","Sales.OrderLines","Sales.Orders"],"source_instance":"sqlserver.WideWorldImporters/v1","snapshot_generation":"wwi-bootstrap-20261028","snapshot_commit_lsn":"00000274:00000758:0003","archive_prefix":"raw/wwi/bootstrap-v2","layout":"table-batch-date/v2"}');

CREATE TABLE dv.h_order_line (
 order_line_hk text PRIMARY KEY, order_line_id bigint NOT NULL UNIQUE,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE TABLE dv.h_invoice (
 invoice_hk text PRIMARY KEY, invoice_id bigint NOT NULL UNIQUE,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE TABLE dv.h_invoice_line (
 invoice_line_hk text PRIMARY KEY, invoice_line_id bigint NOT NULL UNIQUE,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE TABLE dv.l_order_line_order (
 order_line_order_hk text PRIMARY KEY, order_line_hk text NOT NULL REFERENCES dv.h_order_line,
 order_hk text NOT NULL REFERENCES dv.h_order, load_dts timestamptz NOT NULL DEFAULT now(),
 record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters', UNIQUE(order_line_hk,order_hk)
);
CREATE INDEX order_line_order_parent_idx ON dv.l_order_line_order(order_hk);
CREATE TABLE dv.l_invoice_customer (
 invoice_customer_hk text PRIMARY KEY, invoice_hk text NOT NULL REFERENCES dv.h_invoice,
 customer_hk text NOT NULL REFERENCES dv.h_customer, load_dts timestamptz NOT NULL DEFAULT now(),
 record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters', UNIQUE(invoice_hk,customer_hk)
);
CREATE INDEX invoice_customer_parent_idx ON dv.l_invoice_customer(customer_hk);
CREATE TABLE dv.l_invoice_bill_to_customer (
 invoice_bill_to_customer_hk text PRIMARY KEY, invoice_hk text NOT NULL REFERENCES dv.h_invoice,
 customer_hk text NOT NULL REFERENCES dv.h_customer, load_dts timestamptz NOT NULL DEFAULT now(),
 record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters', UNIQUE(invoice_hk,customer_hk)
);
CREATE INDEX invoice_bill_to_parent_idx ON dv.l_invoice_bill_to_customer(customer_hk);
CREATE TABLE dv.l_invoice_order (
 invoice_order_hk text PRIMARY KEY, invoice_hk text NOT NULL REFERENCES dv.h_invoice,
 order_hk text NOT NULL REFERENCES dv.h_order, load_dts timestamptz NOT NULL DEFAULT now(),
 record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters', UNIQUE(invoice_hk,order_hk)
);
CREATE INDEX invoice_order_parent_idx ON dv.l_invoice_order(order_hk);
CREATE TABLE dv.l_invoice_line_invoice (
 invoice_line_invoice_hk text PRIMARY KEY, invoice_line_hk text NOT NULL REFERENCES dv.h_invoice_line,
 invoice_hk text NOT NULL REFERENCES dv.h_invoice, load_dts timestamptz NOT NULL DEFAULT now(),
 record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters', UNIQUE(invoice_line_hk,invoice_hk)
);
CREATE INDEX invoice_line_invoice_parent_idx ON dv.l_invoice_line_invoice(invoice_hk);
CREATE TABLE dv.s_order_line (
 event_id text PRIMARY KEY REFERENCES staging.cdc_event, order_line_hk text NOT NULL REFERENCES dv.h_order_line,
 order_line_order_hk text NOT NULL REFERENCES dv.l_order_line_order,
 hashdiff text NOT NULL, is_deleted boolean NOT NULL, attributes jsonb NOT NULL,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE INDEX sat_order_line_parent_idx ON dv.s_order_line(order_line_hk);
CREATE INDEX sat_order_line_link_idx ON dv.s_order_line(order_line_order_hk);
CREATE TABLE dv.s_invoice (
 event_id text PRIMARY KEY REFERENCES staging.cdc_event, invoice_hk text NOT NULL REFERENCES dv.h_invoice,
 invoice_customer_hk text NOT NULL REFERENCES dv.l_invoice_customer,
 invoice_bill_to_customer_hk text NOT NULL REFERENCES dv.l_invoice_bill_to_customer,
 invoice_order_hk text REFERENCES dv.l_invoice_order,
 hashdiff text NOT NULL, is_deleted boolean NOT NULL, attributes jsonb NOT NULL,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE INDEX sat_invoice_parent_idx ON dv.s_invoice(invoice_hk);
CREATE INDEX sat_invoice_customer_idx ON dv.s_invoice(invoice_customer_hk);
CREATE INDEX sat_invoice_bill_to_idx ON dv.s_invoice(invoice_bill_to_customer_hk);
CREATE INDEX sat_invoice_order_idx ON dv.s_invoice(invoice_order_hk);
CREATE TABLE dv.s_invoice_line (
 event_id text PRIMARY KEY REFERENCES staging.cdc_event, invoice_line_hk text NOT NULL REFERENCES dv.h_invoice_line,
 invoice_line_invoice_hk text NOT NULL REFERENCES dv.l_invoice_line_invoice,
 hashdiff text NOT NULL, is_deleted boolean NOT NULL, attributes jsonb NOT NULL,
 load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE INDEX sat_invoice_line_parent_idx ON dv.s_invoice_line(invoice_line_hk);
CREATE INDEX sat_invoice_line_link_idx ON dv.s_invoice_line(invoice_line_invoice_hk);

-- SQL Server JSON_VALUE scalar semantics: objects/arrays and >4000-character scalars return NULL.
CREATE FUNCTION mart.json_scalar_4000(document jsonb, field_name text) RETURNS text
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT CASE WHEN jsonb_typeof(document->field_name) IN ('string','number','boolean')
 AND length(document->>field_name)<=4000 THEN document->>field_name END
$$;
-- Return text to preserve datetime2(7)'s seventh fractional digit. This is an ISO input
-- contract, not a general T-SQL date parser. Invalid ISO values reproduce TRY_CONVERT NULL.
CREATE FUNCTION mart.try_iso_datetime2_7(value text) RETURNS text
 LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$
DECLARE core text;
BEGIN
 IF value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN value:=value||'T00:00:00'; END IF;
 IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,7})?(Z|[+-][0-9]{2}:[0-9]{2})?$' THEN RETURN NULL; END IF;
 core:=replace(regexp_replace(value,'(Z|[+-][0-9]{2}:[0-9]{2})$',''),' ','T');
 IF substring(core,12,2)::int>23 OR substring(core,15,2)::int>59 OR substring(core,18,2)::int>59 THEN RETURN NULL; END IF;
 PERFORM substring(core,1,19)::timestamp;
 RETURN core;
EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION mart.json_scalar_4000(jsonb,text),mart.try_iso_datetime2_7(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mart.json_scalar_4000(jsonb,text),mart.try_iso_datetime2_7(text) TO warehouse_reader;

CREATE VIEW mart.order_lines AS
 SELECT h.order_line_id,o.order_id,(s.attributes->>'StockItemID')::bigint AS stock_item_id,
 s.attributes->>'Description' AS description,(s.attributes->>'Quantity')::integer AS quantity,
 (s.attributes->>'UnitPrice')::numeric AS unit_price,(s.attributes->>'TaxRate')::numeric AS tax_rate,
 (s.attributes->>'PickedQuantity')::integer AS picked_quantity,s.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_order_line s USING(event_id)
 JOIN dv.h_order_line h USING(order_line_hk) JOIN dv.l_order_line_order l USING(order_line_order_hk)
 JOIN dv.h_order o ON o.order_hk=l.order_hk
 WHERE k.table_name='Sales.OrderLines' AND NOT s.is_deleted;
CREATE VIEW mart.invoices AS
 SELECT h.invoice_id,c.customer_id,b.customer_id AS bill_to_customer_id,o.order_id,
 (s.attributes->>'InvoiceDate')::date AS invoice_date,(s.attributes->>'IsCreditNote')::boolean AS is_credit_note,
 mart.try_iso_datetime2_7(mart.json_scalar_4000((s.attributes->>'ReturnedDeliveryData')::jsonb,'DeliveredWhen')) AS confirmed_delivery_time,
 mart.json_scalar_4000((s.attributes->>'ReturnedDeliveryData')::jsonb,'ReceivedBy') AS confirmed_received_by,
 s.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_invoice s USING(event_id) JOIN dv.h_invoice h USING(invoice_hk)
 JOIN dv.l_invoice_customer lc USING(invoice_customer_hk) JOIN dv.h_customer c ON c.customer_hk=lc.customer_hk
 JOIN dv.l_invoice_bill_to_customer lb USING(invoice_bill_to_customer_hk) JOIN dv.h_customer b ON b.customer_hk=lb.customer_hk
 LEFT JOIN dv.l_invoice_order lo USING(invoice_order_hk) LEFT JOIN dv.h_order o ON o.order_hk=lo.order_hk
 WHERE k.table_name='Sales.Invoices' AND NOT s.is_deleted;
CREATE VIEW mart.invoice_lines AS
 SELECT h.invoice_line_id,i.invoice_id,(s.attributes->>'StockItemID')::bigint AS stock_item_id,
 s.attributes->>'Description' AS description,(s.attributes->>'Quantity')::integer AS quantity,
 (s.attributes->>'UnitPrice')::numeric AS unit_price,(s.attributes->>'TaxRate')::numeric AS tax_rate,
 (s.attributes->>'TaxAmount')::numeric AS tax_amount,(s.attributes->>'LineProfit')::numeric AS line_profit,
 (s.attributes->>'ExtendedPrice')::numeric AS extended_price,s.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_invoice_line s USING(event_id) JOIN dv.h_invoice_line h USING(invoice_line_hk)
 JOIN dv.l_invoice_line_invoice l USING(invoice_line_invoice_hk) JOIN dv.h_invoice i ON i.invoice_hk=l.invoice_hk
 WHERE k.table_name='Sales.InvoiceLines' AND NOT s.is_deleted;
CREATE VIEW mart.order_details AS
 SELECT l.order_line_id,l.order_id,o.customer_id,c.customer_name,o.order_date,
 l.stock_item_id,l.description,l.quantity,l.unit_price,l.tax_rate,l.picked_quantity
 FROM mart.order_lines l LEFT JOIN mart.orders o USING(order_id) LEFT JOIN mart.customers c USING(customer_id);
CREATE VIEW mart.invoice_details AS
 SELECT l.invoice_line_id,l.invoice_id,i.order_id,i.customer_id,c.customer_name,
 i.bill_to_customer_id,b.customer_name AS bill_to_customer_name,i.invoice_date,i.is_credit_note,
 l.stock_item_id,l.description,l.quantity,l.unit_price,l.tax_rate,l.tax_amount,l.line_profit,l.extended_price
 FROM mart.invoice_lines l LEFT JOIN mart.invoices i USING(invoice_id)
 LEFT JOIN mart.customers c ON c.customer_id=i.customer_id
 LEFT JOIN mart.customers b ON b.customer_id=i.bill_to_customer_id;
-- Aggregate at invoice grain before joining; never join order lines to invoice lines by product.
CREATE VIEW mart.invoice_summary AS
 SELECT i.invoice_id,i.order_id,i.customer_id,i.bill_to_customer_id,i.invoice_date,i.is_credit_note,
 coalesce(l.line_count,0) AS line_count,coalesce(l.quantity,0) AS quantity,
 coalesce(l.tax_amount,0) AS tax_amount,coalesce(l.line_profit,0) AS line_profit,
 coalesce(l.extended_price,0) AS extended_price
 FROM mart.invoices i LEFT JOIN (SELECT invoice_id,count(*) AS line_count,sum(quantity) AS quantity,
 sum(tax_amount) AS tax_amount,sum(line_profit) AS line_profit,sum(extended_price) AS extended_price
 FROM mart.invoice_lines GROUP BY invoice_id) l USING(invoice_id);
CREATE VIEW ops.consumer_load_summary AS
 SELECT consumer_version,count(*) AS verified_batches,sum(selected_records) AS selected_event_deliveries,
 sum(new_events) AS new_events,sum(selected_records-new_events) AS reused_event_deliveries,
 sum(ignored_records) AS out_of_scope_events,max(completed_at) AS last_batch_at
 FROM ops.consumer_batches GROUP BY consumer_version;
CREATE OR REPLACE VIEW ops.load_summary AS
 SELECT count(*) AS verified_batches,coalesce(sum(selected_records),0) AS selected_event_deliveries,
 (SELECT count(*) FROM staging.cdc_event) AS distinct_events,coalesce(sum(ignored_records),0) AS out_of_scope_events,
 max(completed_at) AS last_batch_at,false AS purge_authorized
 FROM ops.consumer_batches WHERE consumer_version='sales/v2';
GRANT SELECT,INSERT ON ops.consumer_batches TO warehouse_loader;
GRANT SELECT ON ops.consumer_contracts TO warehouse_loader;
GRANT SELECT,INSERT ON dv.h_order_line,dv.h_invoice,dv.h_invoice_line,dv.l_order_line_order,
 dv.l_invoice_customer,dv.l_invoice_bill_to_customer,dv.l_invoice_order,dv.l_invoice_line_invoice,
 dv.s_order_line,dv.s_invoice,dv.s_invoice_line TO warehouse_loader;
GRANT SELECT ON ALL TABLES IN SCHEMA dv,mart TO warehouse_reader;
GRANT SELECT ON ops.consumer_load_summary TO warehouse_reader;
