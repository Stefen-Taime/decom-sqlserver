-- Additive derived contract. Source events, satellites, hashes and sales/v2 ACKs stay immutable.
CREATE SCHEMA business AUTHORIZATION warehouse_owner;
REVOKE ALL ON SCHEMA business FROM PUBLIC;
CREATE TABLE business.resolution_input (
 event_id text PRIMARY KEY REFERENCES staging.cdc_event(event_id),
 contract_version text NOT NULL CHECK(contract_version='cdc-values/v1'),
 raw_before jsonb NOT NULL,raw_after jsonb NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE business.marker_proof (
 event_id text NOT NULL REFERENCES business.resolution_input(event_id),
 image text NOT NULL CHECK(image IN ('before','after')),column_name text NOT NULL,
 semantic_sha256 text NOT NULL CHECK(length(semantic_sha256)=64),
 proof_sha256 text NOT NULL CHECK(length(proof_sha256)=64),
 source_observation_sha256 text NOT NULL CHECK(length(source_observation_sha256)=64),
 evidence jsonb NOT NULL,recorded_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(event_id,image,column_name)
);
CREATE TABLE business.event_resolution (
 event_id text PRIMARY KEY REFERENCES business.resolution_input(event_id),
 contract_version text NOT NULL CHECK(contract_version='cdc-values/v1'),
 revision_sha256 text NOT NULL CHECK(length(revision_sha256)=64),
 status text NOT NULL CHECK(status IN ('resolved','blocked')),
 resolved_before jsonb NOT NULL,resolved_after jsonb NOT NULL,lineage jsonb NOT NULL,problems jsonb NOT NULL,
 resolved_at timestamptz NOT NULL DEFAULT now(),
 CHECK((status='resolved')=(jsonb_array_length(problems)=0))
);
-- A failed row must fail the query, not vanish from exports through a filtering join.
CREATE FUNCTION business.assert_ready(state text) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF state IS DISTINCT FROM 'resolved' THEN
  RAISE EXCEPTION 'CDC values unresolved; inspect ops.value_resolution_status' USING ERRCODE='55000';
 END IF;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION business.assert_ready(text) FROM PUBLIC;
GRANT USAGE ON SCHEMA business TO warehouse_loader,warehouse_reader;
GRANT SELECT,INSERT ON business.resolution_input TO warehouse_loader;
GRANT SELECT ON business.marker_proof TO warehouse_loader;
GRANT SELECT,INSERT,UPDATE ON business.event_resolution TO warehouse_loader;
GRANT SELECT ON business.event_resolution TO warehouse_reader;
GRANT EXECUTE ON FUNCTION business.assert_ready(text) TO warehouse_reader,warehouse_loader;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='warehouse_exporter') THEN
  GRANT USAGE ON SCHEMA business TO warehouse_exporter;
  GRANT EXECUTE ON FUNCTION business.assert_ready(text) TO warehouse_exporter;
 END IF;
END $$;
CREATE VIEW ops.value_resolution_status AS
 SELECT e.table_name,coalesce(r.status,'pending') AS status,count(*) AS events,
 coalesce(sum(jsonb_array_length(jsonb_path_query_array(r.lineage,'$.*'))),0) AS reconstructed_cells
 FROM business.resolution_input i JOIN staging.cdc_event e USING(event_id)
 LEFT JOIN business.event_resolution r USING(event_id) GROUP BY e.table_name,coalesce(r.status,'pending');
GRANT SELECT ON ops.value_resolution_status TO warehouse_reader,warehouse_loader;

-- Export preflight uses this small index rather than scanning every snapshot row.
CREATE INDEX current_stream_key_idx ON staging.current_key(event_id) WHERE stream_phase=1;
CREATE OR REPLACE VIEW delivery.ingestion_context AS
 SELECT s.consumer_version,s.verified_batches,s.new_events,s.last_batch_at,c.specification,
 false AS source_transaction_boundary_certified,false AS purge_authorized
 FROM ops.consumer_load_summary s JOIN ops.consumer_contracts c USING(consumer_version)
 WHERE s.consumer_version='sales/v2' AND business.assert_ready(CASE WHEN
 EXISTS(SELECT 1 FROM business.resolution_input i JOIN staging.current_key k USING(event_id)
        JOIN staging.cdc_event e USING(event_id) LEFT JOIN business.event_resolution r USING(event_id)
        WHERE e.operation<>'d' AND coalesce(r.status,'pending')<>'resolved')
 OR EXISTS(SELECT 1 FROM staging.current_key k JOIN staging.cdc_event e USING(event_id)
           LEFT JOIN business.event_resolution r USING(event_id)
           WHERE k.stream_phase=1 AND e.operation<>'d' AND coalesce(r.status,'pending')<>'resolved'
           AND jsonb_path_exists(e.row_data,'$.* ? (@ == "__debezium_unavailable_value")'))
 THEN 'blocked' ELSE 'resolved' END);

CREATE OR REPLACE VIEW mart.customers AS
 SELECT h.customer_id, a.attributes->>'CustomerName' AS customer_name,
   (a.attributes->>'BillToCustomerID')::bigint AS bill_to_customer_id,
   (a.attributes->>'CreditLimit')::numeric AS credit_limit,
   (a.attributes->>'IsOnCreditHold')::boolean AS is_on_credit_hold,
   a.attributes, s.load_dts, s.event_id
 FROM staging.current_key k JOIN dv.s_customer s ON s.event_id=k.event_id
 JOIN dv.h_customer h ON h.customer_hk=s.customer_hk

 LEFT JOIN business.event_resolution r ON r.event_id=s.event_id
 CROSS JOIN LATERAL (SELECT CASE WHEN r.status='resolved' THEN r.resolved_after-'CustomerID' ELSE s.attributes END AS attributes) a
 WHERE k.table_name='Sales.Customers' AND CASE WHEN s.is_deleted THEN false
 WHEN r.event_id IS NOT NULL OR jsonb_path_exists(s.attributes,'$.* ? (@ == "__debezium_unavailable_value")')
 THEN business.assert_ready(r.status) ELSE true END;

CREATE OR REPLACE VIEW mart.orders AS
 SELECT h.order_id, c.customer_id, (a.attributes->>'OrderDate')::date AS order_date,
   (a.attributes->>'ExpectedDeliveryDate')::date AS expected_delivery_date,
   a.attributes->>'CustomerPurchaseOrderNumber' AS customer_purchase_order_number,
   a.attributes, s.load_dts, s.event_id
 FROM staging.current_key k JOIN dv.s_order s ON s.event_id=k.event_id
 JOIN dv.h_order h ON h.order_hk=s.order_hk
 JOIN dv.l_order_customer l ON l.order_customer_hk=s.order_customer_hk
 JOIN dv.h_customer c ON c.customer_hk=l.customer_hk

 LEFT JOIN business.event_resolution r ON r.event_id=s.event_id
 CROSS JOIN LATERAL (SELECT CASE WHEN r.status='resolved' THEN r.resolved_after-'OrderID' ELSE s.attributes END AS attributes) a
 WHERE k.table_name='Sales.Orders' AND CASE WHEN s.is_deleted THEN false
 WHEN r.event_id IS NOT NULL OR jsonb_path_exists(s.attributes,'$.* ? (@ == "__debezium_unavailable_value")')
 THEN business.assert_ready(r.status) ELSE true END;

CREATE OR REPLACE VIEW mart.order_lines AS
 SELECT h.order_line_id,o.order_id,(a.attributes->>'StockItemID')::bigint AS stock_item_id,
 a.attributes->>'Description' AS description,(a.attributes->>'Quantity')::integer AS quantity,
 (a.attributes->>'UnitPrice')::numeric AS unit_price,(a.attributes->>'TaxRate')::numeric AS tax_rate,
 (a.attributes->>'PickedQuantity')::integer AS picked_quantity,a.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_order_line s USING(event_id)
 JOIN dv.h_order_line h USING(order_line_hk) JOIN dv.l_order_line_order l USING(order_line_order_hk)
 JOIN dv.h_order o ON o.order_hk=l.order_hk

 LEFT JOIN business.event_resolution r ON r.event_id=s.event_id
 CROSS JOIN LATERAL (SELECT CASE WHEN r.status='resolved' THEN r.resolved_after-'OrderLineID' ELSE s.attributes END AS attributes) a
 WHERE k.table_name='Sales.OrderLines' AND CASE WHEN s.is_deleted THEN false
 WHEN r.event_id IS NOT NULL OR jsonb_path_exists(s.attributes,'$.* ? (@ == "__debezium_unavailable_value")')
 THEN business.assert_ready(r.status) ELSE true END;

CREATE OR REPLACE VIEW mart.invoices AS
 SELECT h.invoice_id,c.customer_id,b.customer_id AS bill_to_customer_id,o.order_id,
 (a.attributes->>'InvoiceDate')::date AS invoice_date,(a.attributes->>'IsCreditNote')::boolean AS is_credit_note,
 mart.try_iso_datetime2_7(mart.json_scalar_4000((a.attributes->>'ReturnedDeliveryData')::jsonb,'DeliveredWhen')) AS confirmed_delivery_time,
 mart.json_scalar_4000((a.attributes->>'ReturnedDeliveryData')::jsonb,'ReceivedBy') AS confirmed_received_by,
 a.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_invoice s USING(event_id) JOIN dv.h_invoice h USING(invoice_hk)
 JOIN dv.l_invoice_customer lc USING(invoice_customer_hk) JOIN dv.h_customer c ON c.customer_hk=lc.customer_hk
 JOIN dv.l_invoice_bill_to_customer lb USING(invoice_bill_to_customer_hk) JOIN dv.h_customer b ON b.customer_hk=lb.customer_hk
 LEFT JOIN dv.l_invoice_order lo USING(invoice_order_hk) LEFT JOIN dv.h_order o ON o.order_hk=lo.order_hk

 LEFT JOIN business.event_resolution r ON r.event_id=s.event_id
 CROSS JOIN LATERAL (SELECT CASE WHEN r.status='resolved' THEN r.resolved_after-'InvoiceID' ELSE s.attributes END AS attributes) a
 WHERE k.table_name='Sales.Invoices' AND CASE WHEN s.is_deleted THEN false
 WHEN r.event_id IS NOT NULL OR jsonb_path_exists(s.attributes,'$.* ? (@ == "__debezium_unavailable_value")')
 THEN business.assert_ready(r.status) ELSE true END;

CREATE OR REPLACE VIEW mart.invoice_lines AS
 SELECT h.invoice_line_id,i.invoice_id,(a.attributes->>'StockItemID')::bigint AS stock_item_id,
 a.attributes->>'Description' AS description,(a.attributes->>'Quantity')::integer AS quantity,
 (a.attributes->>'UnitPrice')::numeric AS unit_price,(a.attributes->>'TaxRate')::numeric AS tax_rate,
 (a.attributes->>'TaxAmount')::numeric AS tax_amount,(a.attributes->>'LineProfit')::numeric AS line_profit,
 (a.attributes->>'ExtendedPrice')::numeric AS extended_price,a.attributes,s.load_dts,s.event_id
 FROM staging.current_key k JOIN dv.s_invoice_line s USING(event_id) JOIN dv.h_invoice_line h USING(invoice_line_hk)
 JOIN dv.l_invoice_line_invoice l USING(invoice_line_invoice_hk) JOIN dv.h_invoice i ON i.invoice_hk=l.invoice_hk

 LEFT JOIN business.event_resolution r ON r.event_id=s.event_id
 CROSS JOIN LATERAL (SELECT CASE WHEN r.status='resolved' THEN r.resolved_after-'InvoiceLineID' ELSE s.attributes END AS attributes) a
 WHERE k.table_name='Sales.InvoiceLines' AND CASE WHEN s.is_deleted THEN false
 WHEN r.event_id IS NOT NULL OR jsonb_path_exists(s.attributes,'$.* ? (@ == "__debezium_unavailable_value")')
 THEN business.assert_ready(r.status) ELSE true END;
