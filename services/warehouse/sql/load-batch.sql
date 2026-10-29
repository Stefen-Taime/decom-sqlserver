-- incoming contains only validated pilot events. A transaction-level lock serializes writers.
CREATE TEMP TABLE new_events ON COMMIT DROP AS
 WITH inserted AS (INSERT INTO staging.cdc_event SELECT * FROM incoming ON CONFLICT(event_id) DO NOTHING RETURNING *)
 SELECT * FROM inserted;

INSERT INTO dv.h_customer(customer_hk,customer_id)
 SELECT DISTINCT customer_hk, CASE WHEN table_name='Sales.Customers' THEN source_pk ELSE (row_data->>'CustomerID')::bigint END
 FROM new_events WHERE customer_hk IS NOT NULL
 UNION SELECT DISTINCT bill_to_customer_hk,(row_data->>'BillToCustomerID')::bigint
 FROM new_events WHERE bill_to_customer_hk IS NOT NULL ON CONFLICT DO NOTHING;
INSERT INTO dv.h_order(order_hk,order_id)
 SELECT DISTINCT order_hk,CASE WHEN table_name='Sales.Orders' THEN source_pk ELSE (row_data->>'OrderID')::bigint END
 FROM new_events WHERE order_hk IS NOT NULL ON CONFLICT DO NOTHING;
INSERT INTO dv.h_order_line(order_line_hk,order_line_id)
 SELECT DISTINCT entity_hk,source_pk FROM new_events WHERE table_name='Sales.OrderLines' ON CONFLICT DO NOTHING;
INSERT INTO dv.h_invoice(invoice_hk,invoice_id)
 SELECT DISTINCT invoice_hk,CASE WHEN table_name='Sales.Invoices' THEN source_pk ELSE (row_data->>'InvoiceID')::bigint END
 FROM new_events WHERE invoice_hk IS NOT NULL ON CONFLICT DO NOTHING;
INSERT INTO dv.h_invoice_line(invoice_line_hk,invoice_line_id)
 SELECT DISTINCT entity_hk,source_pk FROM new_events WHERE table_name='Sales.InvoiceLines' ON CONFLICT DO NOTHING;
INSERT INTO dv.l_order_customer(order_customer_hk,order_hk,customer_hk)
 SELECT DISTINCT link_hk,entity_hk,customer_hk FROM new_events WHERE table_name='Sales.Orders' ON CONFLICT DO NOTHING;
INSERT INTO dv.l_order_line_order(order_line_order_hk,order_line_hk,order_hk)
 SELECT DISTINCT link_hk,entity_hk,order_hk FROM new_events WHERE table_name='Sales.OrderLines' ON CONFLICT DO NOTHING;
INSERT INTO dv.l_invoice_customer(invoice_customer_hk,invoice_hk,customer_hk)
 SELECT DISTINCT link_hk,entity_hk,customer_hk FROM new_events WHERE table_name='Sales.Invoices' ON CONFLICT DO NOTHING;
INSERT INTO dv.l_invoice_bill_to_customer(invoice_bill_to_customer_hk,invoice_hk,customer_hk)
 SELECT DISTINCT bill_to_link_hk,entity_hk,bill_to_customer_hk FROM new_events WHERE table_name='Sales.Invoices' ON CONFLICT DO NOTHING;
INSERT INTO dv.l_invoice_order(invoice_order_hk,invoice_hk,order_hk)
 SELECT DISTINCT order_link_hk,entity_hk,order_hk FROM new_events WHERE table_name='Sales.Invoices' AND order_link_hk IS NOT NULL ON CONFLICT DO NOTHING;
INSERT INTO dv.l_invoice_line_invoice(invoice_line_invoice_hk,invoice_line_hk,invoice_hk)
 SELECT DISTINCT link_hk,entity_hk,invoice_hk FROM new_events WHERE table_name='Sales.InvoiceLines' ON CONFLICT DO NOTHING;
INSERT INTO dv.s_customer(event_id,customer_hk,hashdiff,is_deleted,attributes)
 SELECT event_id,entity_hk,hashdiff,operation='d',row_data-'CustomerID'
 FROM new_events WHERE table_name='Sales.Customers';
INSERT INTO dv.s_order(event_id,order_hk,order_customer_hk,hashdiff,is_deleted,attributes)
 SELECT event_id,entity_hk,link_hk,hashdiff,operation='d',row_data-'OrderID'
 FROM new_events WHERE table_name='Sales.Orders';
INSERT INTO dv.s_order_line(event_id,order_line_hk,order_line_order_hk,hashdiff,is_deleted,attributes)
 SELECT event_id,entity_hk,link_hk,hashdiff,operation='d',row_data-'OrderLineID'
 FROM new_events WHERE table_name='Sales.OrderLines';
INSERT INTO dv.s_invoice(event_id,invoice_hk,invoice_customer_hk,invoice_bill_to_customer_hk,invoice_order_hk,hashdiff,is_deleted,attributes)
 SELECT event_id,entity_hk,link_hk,bill_to_link_hk,order_link_hk,hashdiff,operation='d',row_data-'InvoiceID'
 FROM new_events WHERE table_name='Sales.Invoices';
INSERT INTO dv.s_invoice_line(event_id,invoice_line_hk,invoice_line_invoice_hk,hashdiff,is_deleted,attributes)
 SELECT event_id,entity_hk,link_hk,hashdiff,operation='d',row_data-'InvoiceLineID'
 FROM new_events WHERE table_name='Sales.InvoiceLines';
INSERT INTO staging.current_key AS current(table_name,source_pk,event_id,stream_phase,commit_lsn,change_lsn,event_serial_no)
 SELECT DISTINCT ON(table_name,source_pk) table_name,source_pk,event_id,stream_phase,commit_lsn,change_lsn,event_serial_no
 FROM new_events ORDER BY table_name,source_pk,stream_phase DESC,commit_lsn DESC,change_lsn DESC,event_serial_no DESC
 ON CONFLICT(table_name,source_pk) DO UPDATE SET event_id=excluded.event_id,stream_phase=excluded.stream_phase,
 commit_lsn=excluded.commit_lsn,change_lsn=excluded.change_lsn,event_serial_no=excluded.event_serial_no
 WHERE (excluded.stream_phase,excluded.commit_lsn,excluded.change_lsn,excluded.event_serial_no)>
       (current.stream_phase,current.commit_lsn,current.change_lsn,current.event_serial_no);
