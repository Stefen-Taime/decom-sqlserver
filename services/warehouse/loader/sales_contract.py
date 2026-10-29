"""Versioned five-table scope and explicit cross-engine reconciliation projections."""
CONSUMER_VERSION = 'sales/v2'
SOURCE_INSTANCE = 'sqlserver.WideWorldImporters/v1'
SNAPSHOT_GENERATION = 'wwi-bootstrap-20261028'
SNAPSHOT_COMMIT_LSN = '00000274:00000758:0003'
ARCHIVE_PREFIX = 'raw/wwi/bootstrap-v2'
TABLE_SPECS = {
    'Sales.Customers': {'pk':'CustomerID','entity':'customer','view':'customers','id_column':'customer_id',
        'columns':'CustomerID,CustomerName,BillToCustomerID,CustomerCategoryID,BuyingGroupID,PrimaryContactPersonID,AlternateContactPersonID,DeliveryMethodID,DeliveryCityID,PostalCityID,CreditLimit,AccountOpenedDate,StandardDiscountPercentage,IsStatementSent,IsOnCreditHold,PaymentDays,PhoneNumber,FaxNumber,DeliveryRun,RunPosition,WebsiteURL,DeliveryAddressLine1,DeliveryAddressLine2,DeliveryPostalCode,PostalAddressLine1,PostalAddressLine2,PostalPostalCode'},
    'Sales.Orders': {'pk':'OrderID','entity':'order','view':'orders','id_column':'order_id',
        'columns':'OrderID,CustomerID,SalespersonPersonID,PickedByPersonID,ContactPersonID,BackorderOrderID,OrderDate,ExpectedDeliveryDate,CustomerPurchaseOrderNumber,IsUndersupplyBackordered,Comments,DeliveryInstructions,InternalComments,PickingCompletedWhen'},
    'Sales.OrderLines': {'pk':'OrderLineID','entity':'order-line','view':'order_lines','id_column':'order_line_id',
        'columns':'OrderLineID,OrderID,StockItemID,Description,PackageTypeID,Quantity,UnitPrice,TaxRate,PickedQuantity,PickingCompletedWhen'},
    'Sales.Invoices': {'pk':'InvoiceID','entity':'invoice','view':'invoices','id_column':'invoice_id',
        'columns':'InvoiceID,CustomerID,BillToCustomerID,OrderID,DeliveryMethodID,ContactPersonID,AccountsPersonID,SalespersonPersonID,PackedByPersonID,InvoiceDate,CustomerPurchaseOrderNumber,IsCreditNote,CreditNoteReason,Comments,DeliveryInstructions,InternalComments,TotalDryItems,TotalChillerItems,DeliveryRun,RunPosition,ReturnedDeliveryData,ConfirmedDeliveryTime,ConfirmedReceivedBy'},
    'Sales.InvoiceLines': {'pk':'InvoiceLineID','entity':'invoice-line','view':'invoice_lines','id_column':'invoice_line_id',
        'columns':'InvoiceLineID,InvoiceID,StockItemID,Description,PackageTypeID,Quantity,UnitPrice,TaxRate,TaxAmount,LineProfit,ExtendedPrice'},
}
EXPECTED_COUNTS = {'Sales.Customers':663,'Sales.Orders':73595,'Sales.OrderLines':231412,'Sales.Invoices':70510,'Sales.InvoiceLines':228265}
