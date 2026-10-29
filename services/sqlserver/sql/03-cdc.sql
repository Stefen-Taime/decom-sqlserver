USE WideWorldImporters;
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE database_id = DB_ID() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO
IF DATABASE_PRINCIPAL_ID(N'cdc_reader') IS NULL
    CREATE ROLE cdc_reader;

IF SCHEMA_ID(N'lab') IS NULL
    EXEC(N'CREATE SCHEMA lab AUTHORIZATION dbo;');

-- Isolated test table: the smoke test does not modify sample business records.
IF OBJECT_ID(N'lab.CdcProbe', N'U') IS NULL
    CREATE TABLE lab.CdcProbe (
        ProbeID uniqueidentifier NOT NULL CONSTRAINT PK_Lab_CdcProbe PRIMARY KEY,
        Payload nvarchar(100) NOT NULL
    );
GO
DECLARE @targets TABLE (SchemaName sysname, TableName sysname);
INSERT INTO @targets VALUES
    (N'Sales', N'Customers'),
    (N'Sales', N'Orders'),
    (N'Sales', N'OrderLines'),
    (N'Sales', N'Invoices'),
    (N'Sales', N'InvoiceLines'),
    (N'lab', N'CdcProbe');

DECLARE @schema sysname, @table sysname, @object_id int;
DECLARE tables_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT SchemaName, TableName FROM @targets;
OPEN tables_cursor;
FETCH NEXT FROM tables_cursor INTO @schema, @table;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @object_id = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table), N'U');
    IF @object_id IS NULL
        THROW 51001, 'Expected source table is missing.', 1;

    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = @object_id AND is_tracked_by_cdc = 1)
        EXEC sys.sp_cdc_enable_table
            @source_schema = @schema,
            @source_name = @table,
            @role_name = N'cdc_reader',
            @supports_net_changes = 0;

    FETCH NEXT FROM tables_cursor INTO @schema, @table;
END
CLOSE tables_cursor;
DEALLOCATE tables_cursor;
GO
-- Disable schedule-based cleanup until archive/checkpoint-aware purge exists.
-- Stopping a running job alone would not prevent its next scheduled execution.
DECLARE @cleanup_job uniqueidentifier;
SELECT @cleanup_job = job_id FROM msdb.dbo.cdc_jobs
WHERE database_id = DB_ID() AND job_type = N'cleanup';
IF @cleanup_job IS NULL
    THROW 51002, 'CDC cleanup job was not created.', 1;

EXEC msdb.dbo.sp_update_job @job_id = @cleanup_job, @enabled = 0;
IF EXISTS (
    SELECT 1 FROM msdb.dbo.sysjobactivity
    WHERE job_id = @cleanup_job
      AND session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
      AND start_execution_date IS NOT NULL AND stop_execution_date IS NULL
)
    EXEC msdb.dbo.sp_stop_job @job_id = @cleanup_job;
GO
