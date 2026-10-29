-- Applied once, transactionally, as warehouse_owner. Append future migrations; do not edit applied SQL.
CREATE SCHEMA ops AUTHORIZATION warehouse_owner;
CREATE SCHEMA staging AUTHORIZATION warehouse_owner;
CREATE SCHEMA dv AUTHORIZATION warehouse_owner;
CREATE SCHEMA mart AUTHORIZATION warehouse_owner;

CREATE TABLE ops.schema_migrations (
  version text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE ops.archive_batches (
  batch_id text PRIMARY KEY, manifest_key text NOT NULL UNIQUE, manifest_sha256 text NOT NULL,
  payload_key text NOT NULL, archive_prefix text NOT NULL,
  total_records integer NOT NULL CHECK(total_records>0), selected_records integer NOT NULL,
  new_events integer NOT NULL, ignored_records integer NOT NULL,
  consumer_version text NOT NULL, completed_at timestamptz NOT NULL DEFAULT now(),
  CHECK (selected_records+ignored_records=total_records),
  CHECK (new_events>=0 AND new_events<=selected_records)
);
CREATE TABLE ops.consumer_status (
  consumer text PRIMARY KEY, last_success_at timestamptz, last_error_at timestamptz,
  error_type text, scanned_manifests integer, poll_seconds integer NOT NULL DEFAULT 30
);
CREATE TABLE staging.cdc_event (
  event_id text PRIMARY KEY CHECK(length(event_id)=64), semantic_sha256 text NOT NULL,
  table_name text NOT NULL CHECK(table_name IN ('Sales.Customers','Sales.Orders')),
  source_pk bigint NOT NULL, operation char(1) NOT NULL CHECK(operation IN ('r','c','u','d')),
  stream_phase smallint NOT NULL CHECK(stream_phase IN (0,1)),
  commit_lsn bytea NOT NULL CHECK(octet_length(commit_lsn)=10),
  change_lsn bytea NOT NULL CHECK(octet_length(change_lsn)=10), event_serial_no bigint NOT NULL,
  source_ts_ms bigint, capture_ts_ms bigint NOT NULL,
  entity_hk text NOT NULL, customer_hk text NOT NULL, link_hk text,
  hashdiff text NOT NULL, row_data jsonb NOT NULL, source_metadata jsonb NOT NULL,
  first_batch_id text NOT NULL REFERENCES ops.archive_batches(batch_id) DEFERRABLE INITIALLY DEFERRED,
  load_dts timestamptz NOT NULL DEFAULT now(),
  UNIQUE(table_name,source_pk,stream_phase,commit_lsn,change_lsn,event_serial_no)
);
CREATE INDEX cdc_event_batch_idx ON staging.cdc_event(first_batch_id);
CREATE TABLE staging.current_key (
  table_name text NOT NULL, source_pk bigint NOT NULL,
  event_id text NOT NULL REFERENCES staging.cdc_event(event_id),
  stream_phase smallint NOT NULL, commit_lsn bytea NOT NULL, change_lsn bytea NOT NULL, event_serial_no bigint NOT NULL,
  PRIMARY KEY(table_name,source_pk)
);
CREATE INDEX current_key_event_idx ON staging.current_key(event_id);

CREATE TABLE dv.h_customer (
  customer_hk text PRIMARY KEY, customer_id bigint NOT NULL UNIQUE,
  load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE TABLE dv.h_order (
  order_hk text PRIMARY KEY, order_id bigint NOT NULL UNIQUE,
  load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE TABLE dv.l_order_customer (
  order_customer_hk text PRIMARY KEY,
  order_hk text NOT NULL REFERENCES dv.h_order(order_hk), customer_hk text NOT NULL REFERENCES dv.h_customer(customer_hk),
  load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters',
  UNIQUE(order_hk,customer_hk)
);
CREATE INDEX link_customer_idx ON dv.l_order_customer(customer_hk);
CREATE TABLE dv.s_customer (
  event_id text PRIMARY KEY REFERENCES staging.cdc_event(event_id),
  customer_hk text NOT NULL REFERENCES dv.h_customer(customer_hk),
  hashdiff text NOT NULL, is_deleted boolean NOT NULL, attributes jsonb NOT NULL,
  load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE INDEX sat_customer_parent_idx ON dv.s_customer(customer_hk);
CREATE TABLE dv.s_order (
  event_id text PRIMARY KEY REFERENCES staging.cdc_event(event_id),
  order_hk text NOT NULL REFERENCES dv.h_order(order_hk),
  order_customer_hk text NOT NULL REFERENCES dv.l_order_customer(order_customer_hk),
  hashdiff text NOT NULL, is_deleted boolean NOT NULL, attributes jsonb NOT NULL,
  load_dts timestamptz NOT NULL DEFAULT now(), record_source text NOT NULL DEFAULT 'sqlserver.WideWorldImporters'
);
CREATE INDEX sat_order_parent_idx ON dv.s_order(order_hk);
CREATE INDEX sat_order_link_idx ON dv.s_order(order_customer_hk);

CREATE VIEW mart.customers AS
 SELECT h.customer_id, s.attributes->>'CustomerName' AS customer_name,
   (s.attributes->>'BillToCustomerID')::bigint AS bill_to_customer_id,
   (s.attributes->>'CreditLimit')::numeric AS credit_limit,
   (s.attributes->>'IsOnCreditHold')::boolean AS is_on_credit_hold,
   s.attributes, s.load_dts, s.event_id
 FROM staging.current_key k JOIN dv.s_customer s ON s.event_id=k.event_id
 JOIN dv.h_customer h ON h.customer_hk=s.customer_hk
 WHERE k.table_name='Sales.Customers' AND NOT s.is_deleted;
CREATE VIEW mart.orders AS
 SELECT h.order_id, c.customer_id, (s.attributes->>'OrderDate')::date AS order_date,
   (s.attributes->>'ExpectedDeliveryDate')::date AS expected_delivery_date,
   s.attributes->>'CustomerPurchaseOrderNumber' AS customer_purchase_order_number,
   s.attributes, s.load_dts, s.event_id
 FROM staging.current_key k JOIN dv.s_order s ON s.event_id=k.event_id
 JOIN dv.h_order h ON h.order_hk=s.order_hk
 JOIN dv.l_order_customer l ON l.order_customer_hk=s.order_customer_hk
 JOIN dv.h_customer c ON c.customer_hk=l.customer_hk
 WHERE k.table_name='Sales.Orders' AND NOT s.is_deleted;
CREATE VIEW ops.load_summary AS
 SELECT count(*) AS verified_batches, coalesce(sum(selected_records),0) AS selected_event_deliveries,
 coalesce(sum(new_events),0) AS distinct_events, coalesce(sum(ignored_records),0) AS out_of_scope_events,
 max(completed_at) AS last_batch_at, false AS purge_authorized FROM ops.archive_batches;

GRANT USAGE ON SCHEMA ops,staging,dv TO warehouse_loader;
GRANT SELECT,INSERT ON ops.archive_batches,staging.cdc_event,staging.current_key,
  dv.h_customer,dv.h_order,dv.l_order_customer,dv.s_customer,dv.s_order TO warehouse_loader;
GRANT UPDATE ON staging.current_key TO warehouse_loader;
GRANT SELECT,INSERT,UPDATE ON ops.consumer_status TO warehouse_loader;
GRANT SELECT ON ops.schema_migrations,ops.load_summary TO warehouse_loader;
GRANT USAGE ON SCHEMA dv,mart,ops TO warehouse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dv,mart TO warehouse_reader;
GRANT SELECT ON ops.load_summary,ops.consumer_status TO warehouse_reader;
