USE master;
IF SUSER_ID(N'debezium_cdc') IS NULL
    CREATE LOGIN debezium_cdc WITH PASSWORD = N'{{PASSWORD}}', CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
GRANT VIEW SERVER STATE TO debezium_cdc;
GRANT VIEW SERVER SECURITY STATE TO debezium_cdc;
GO
USE WideWorldImporters;
IF USER_ID(N'debezium_cdc') IS NULL
    CREATE USER debezium_cdc FOR LOGIN debezium_cdc;
IF IS_ROLEMEMBER(N'cdc_reader', N'debezium_cdc') <> 1
    ALTER ROLE cdc_reader ADD MEMBER debezium_cdc;
GRANT CONNECT TO debezium_cdc;
GRANT VIEW DEFINITION TO debezium_cdc;
GRANT SELECT ON SCHEMA::cdc TO cdc_reader;
GRANT SELECT ON OBJECT::Sales.Customers TO cdc_reader;
GRANT SELECT ON OBJECT::Sales.Orders TO cdc_reader;
GRANT SELECT ON OBJECT::Sales.OrderLines TO cdc_reader;
GRANT SELECT ON OBJECT::Sales.Invoices TO cdc_reader;
GRANT SELECT ON OBJECT::Sales.InvoiceLines TO cdc_reader;
GRANT SELECT ON OBJECT::lab.CdcProbe TO cdc_reader;
GO
