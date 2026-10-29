-- Additive read API. Source objects, CDC ingestion and previous migrations stay intact.
CREATE SCHEMA app AUTHORIZATION warehouse_owner;
CREATE SCHEMA ref AUTHORIZATION warehouse_owner;
CREATE TABLE ops.reference_snapshots (
 snapshot_version text PRIMARY KEY, source_table text NOT NULL,
 projection jsonb NOT NULL, payload_sha256 text NOT NULL CHECK(length(payload_sha256)=64),
 source_captured_at timestamptz NOT NULL, row_count integer NOT NULL CHECK(row_count>0),
 loaded_at timestamptz NOT NULL DEFAULT now(),
 refresh_mode text NOT NULL DEFAULT 'explicit_versioned_snapshot' CHECK(refresh_mode='explicit_versioned_snapshot')
);
CREATE TABLE ref.package_types (
 snapshot_version text NOT NULL REFERENCES ops.reference_snapshots,
 package_type_id integer NOT NULL, package_type_name varchar(50) NOT NULL,
 source_valid_from text NOT NULL, source_valid_to text NOT NULL,
 PRIMARY KEY(snapshot_version,package_type_id)
);

-- Canonical wall-clock text preserves SQL Server datetime2(7) exactly.
-- Public cutoff inputs accept ISO dates / seconds with 0..7 fractional digits, without a timezone.
CREATE FUNCTION app.datetime2_7(value text, required boolean DEFAULT false) RETURNS text
 LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE core text; fraction text;
BEGIN
 IF value IS NULL THEN
  IF required THEN RAISE EXCEPTION 'Required source datetime2 value is missing' USING ERRCODE='22004'; END IF;
  RETURN NULL;
 END IF;
 IF value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN value:=value||'T00:00:00'; END IF;
 IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,7})?$' THEN
  RAISE EXCEPTION 'Expected ISO datetime2 with at most seven fractional digits and no timezone' USING ERRCODE='22007';
 END IF;
 core:=replace(substring(value,1,19),' ','T');
 IF substring(core,1,4)::int=0 OR substring(core,12,2)::int>23 OR substring(core,15,2)::int>59 OR substring(core,18,2)::int>59 THEN
  RAISE EXCEPTION 'Invalid datetime2 calendar or clock value' USING ERRCODE='22007';
 END IF;
 PERFORM core::timestamp;
 fraction:=CASE WHEN length(value)>19 THEN substring(value,21) ELSE '' END;
 RETURN core||'.'||rpad(fraction,7,'0');
END $$;

CREATE FUNCTION app.order_line_amounts(quantity integer, unit_price numeric, tax_rate numeric)
 RETURNS TABLE(total_excluding_tax numeric,tax_amount numeric,total_including_tax numeric)
 LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH inputs AS (SELECT unit_price::numeric(18,2) AS price,tax_rate::numeric(18,3) AS tax),
 base AS (SELECT (quantity::numeric(10,0)*price)::numeric(29,2) AS amount,tax FROM inputs),
 amounts AS (SELECT round(amount,2)::numeric(29,2) AS net,
  round(((amount*tax)::numeric(38,5)/100.0)::numeric(38,6),2)::numeric(38,6) AS vat FROM base)
 SELECT net,vat,(net+vat)::numeric(38,6) FROM amounts
$$;

CREATE FUNCTION app.missing_package_type(package_type_id integer) RETURNS text
 LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION 'Package type % is missing from the requested reference snapshot',package_type_id USING ERRCODE='23503';
END $$;

CREATE FUNCTION app.get_order_updates(last_cutoff text,new_cutoff text,reference_version text)
 RETURNS TABLE("Order Date Key" date,"Picked Date Key" date,"WWI Order ID" integer,"WWI Backorder ID" integer,
 "Description" text,"Package" text,"Quantity" integer,"Unit Price" numeric,"Tax Rate" numeric,
 "Total Excluding Tax" numeric,"Tax Amount" numeric,"Total Including Tax" numeric,"WWI City ID" integer,
 "WWI Customer ID" integer,"WWI Stock Item ID" integer,"WWI Salesperson ID" integer,"WWI Picker ID" integer,
 "Last Modified When" text)
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE lower_bound text; upper_bound text;
BEGIN
 lower_bound:=app.datetime2_7(last_cutoff);
 upper_bound:=app.datetime2_7(new_cutoff);
 IF reference_version IS NULL OR NOT EXISTS (
  SELECT 1 FROM ops.reference_snapshots r WHERE r.snapshot_version=reference_version
   AND r.source_table='Warehouse.PackageTypes'
   AND r.row_count=(SELECT count(*) FROM ref.package_types p WHERE p.snapshot_version=r.snapshot_version)
 ) THEN RAISE EXCEPTION 'Unknown or incomplete package-type snapshot' USING ERRCODE='22023'; END IF;
 IF lower_bound IS NULL OR upper_bound IS NULL OR lower_bound COLLATE "C">=upper_bound COLLATE "C" THEN RETURN; END IF;
 RETURN QUERY
 WITH base AS MATERIALIZED (
  SELECT o.order_id,o.customer_id,o.order_date,o.attributes AS oa,l.order_line_id,l.attributes AS la,
   app.datetime2_7(regexp_replace(o.attributes->>'LastEditedWhen','Z$',''),true) AS order_edited,
   app.datetime2_7(regexp_replace(l.attributes->>'LastEditedWhen','Z$',''),true) AS line_edited
  FROM mart.orders o JOIN mart.order_lines l USING(order_id)
 ), eligible AS (
  SELECT b.*,CASE WHEN line_edited COLLATE "C">order_edited COLLATE "C" THEN line_edited ELSE order_edited END AS modified
  FROM base b
 )
 SELECT b.order_date,substring(b.la->>'PickingCompletedWhen',1,10)::date,b.order_id::integer,(b.oa->>'BackorderOrderID')::integer,
  b.la->>'Description',coalesce(p.package_type_name::text,app.missing_package_type((b.la->>'PackageTypeID')::integer)),
  (b.la->>'Quantity')::integer,(b.la->>'UnitPrice')::numeric(18,2),(b.la->>'TaxRate')::numeric(18,3),
  a.total_excluding_tax,a.tax_amount,a.total_including_tax,(c.attributes->>'DeliveryCityID')::integer,
  b.customer_id::integer,(b.la->>'StockItemID')::integer,(b.oa->>'SalespersonPersonID')::integer,(b.oa->>'PickedByPersonID')::integer,b.modified
 FROM eligible b JOIN mart.customers c USING(customer_id)
 LEFT JOIN ref.package_types p ON p.snapshot_version=reference_version AND p.package_type_id=(b.la->>'PackageTypeID')::integer
 CROSS JOIN LATERAL app.order_line_amounts((b.la->>'Quantity')::integer,(b.la->>'UnitPrice')::numeric,(b.la->>'TaxRate')::numeric) a
 WHERE b.modified COLLATE "C">lower_bound COLLATE "C" AND b.modified COLLATE "C"<=upper_bound COLLATE "C"
 ORDER BY b.order_id,b.order_line_id;
END $$;

COMMENT ON FUNCTION app.get_order_updates(text,text,text) IS
 'Pilot port of Integration.GetOrderUpdates. Window (last,new], exact datetime2(7) ISO text, explicit immutable PackageTypes snapshot. Current-row extraction, not a deletion feed or cutover watermark. Source RLS/application authorization requires separate review.';
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
GRANT USAGE ON SCHEMA app,ref TO warehouse_reader;
GRANT SELECT ON ops.reference_snapshots,ref.package_types TO warehouse_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA app TO warehouse_reader;
