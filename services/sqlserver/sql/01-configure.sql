USE master;
GO
EXEC sys.sp_configure N'show advanced options', 1;
RECONFIGURE;
EXEC sys.sp_configure N'max server memory (MB)', 2048;
EXEC sys.sp_configure N'max degree of parallelism', 2;
RECONFIGURE;
GO
