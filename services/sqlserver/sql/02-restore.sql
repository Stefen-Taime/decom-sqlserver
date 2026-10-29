USE master;
GO
IF DB_ID(N'WideWorldImporters') IS NULL
BEGIN
    RESTORE VERIFYONLY
        FROM DISK = N'/var/opt/mssql/backup/WideWorldImporters-Full.bak';

    RESTORE DATABASE WideWorldImporters
        FROM DISK = N'/var/opt/mssql/backup/WideWorldImporters-Full.bak'
        WITH MOVE N'WWI_Primary' TO N'/var/opt/mssql/data/WideWorldImporters.mdf',
             MOVE N'WWI_UserData' TO N'/var/opt/mssql/data/WideWorldImporters_userdata.ndf',
             MOVE N'WWI_Log' TO N'/var/opt/mssql/data/WideWorldImporters.ldf',
             MOVE N'WWI_InMemory_Data_1' TO N'/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1',
             STATS = 10;
END
ELSE
    PRINT N'WideWorldImporters already exists; restore skipped.';
GO
DECLARE @deadline datetime2 = DATEADD(second, 180, SYSUTCDATETIME());
WHILE NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'WideWorldImporters' AND state = 0)
      AND SYSUTCDATETIME() < @deadline
    WAITFOR DELAY '00:00:02';

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'WideWorldImporters' AND state = 0)
    THROW 51000, 'WideWorldImporters is not online.', 1;

-- Replace the sample backup's Windows owner SID with this lab's administrator.
ALTER AUTHORIZATION ON DATABASE::WideWorldImporters TO sa;
GO
