SET NOCOUNT ON;
USE WideWorldImporters;
GO
IF NOT EXISTS (SELECT 1 FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%' AND status = 4)
    THROW 51003, 'SQL Server Agent is not running.', 1;

IF (SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance IN
    (N'Sales_Customers', N'Sales_Orders', N'Sales_OrderLines', N'Sales_Invoices', N'Sales_InvoiceLines', N'lab_CdcProbe')) <> 6
    THROW 51004, 'Expected CDC capture instances are missing.', 1;

IF NOT EXISTS (
    SELECT 1 FROM msdb.dbo.cdc_jobs c
    JOIN msdb.dbo.sysjobs j ON j.job_id = c.job_id
    WHERE c.database_id = DB_ID() AND c.job_type = N'cleanup' AND j.enabled = 0
)
    THROW 51005, 'Automatic CDC cleanup must remain disabled.', 1;

IF NOT EXISTS (SELECT 1 FROM Sales.Customers)
    OR NOT EXISTS (SELECT 1 FROM Sales.Orders)
    OR NOT EXISTS (SELECT 1 FROM Sales.Invoices)
    THROW 51006, 'Sample business data is missing.', 1;

SELECT CONVERT(varchar(32), SERVERPROPERTY('ProductVersion')) AS ProductVersion,
       CONVERT(varchar(64), SERVERPROPERTY('Edition')) AS Edition;
SELECT CONVERT(varchar(100), servicename) AS Service, status_desc FROM sys.dm_server_services;
SELECT name, CONVERT(int, value_in_use) AS Value FROM sys.configurations
WHERE name IN (N'max server memory (MB)', N'max degree of parallelism');
SELECT N'Customers' AS Entity, COUNT_BIG(*) AS Rows FROM Sales.Customers
UNION ALL SELECT N'Orders', COUNT_BIG(*) FROM Sales.Orders
UNION ALL SELECT N'OrderLines', COUNT_BIG(*) FROM Sales.OrderLines
UNION ALL SELECT N'Invoices', COUNT_BIG(*) FROM Sales.Invoices
UNION ALL SELECT N'InvoiceLines', COUNT_BIG(*) FROM Sales.InvoiceLines;
SELECT type_desc, COUNT(*) AS ObjectCount FROM sys.objects
WHERE is_ms_shipped = 0 AND schema_id NOT IN (SCHEMA_ID(N'cdc'), SCHEMA_ID(N'lab'))
  AND type IN (N'U', N'P', N'PC', N'V', N'FN', N'IF', N'TF', N'TR')
GROUP BY type_desc ORDER BY type_desc;
SELECT capture_instance FROM cdc.change_tables ORDER BY capture_instance;
SELECT c.job_type, j.name, j.enabled FROM msdb.dbo.cdc_jobs c
JOIN msdb.dbo.sysjobs j ON j.job_id = c.job_id
WHERE c.database_id = DB_ID();
GO
