SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%' AND status = 4)
    THROW 51008, 'SQL Server Agent is not ready.', 1;

-- The engine may accept master connections while the user database recovers.
IF DB_ID(N'WideWorldImporters') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'WideWorldImporters' AND state = 0)
    THROW 51009, 'WideWorldImporters is not ready.', 1;

SELECT 1;
