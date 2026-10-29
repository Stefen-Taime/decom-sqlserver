USE master;
IF SUSER_ID(N'lab_reader') IS NULL
    CREATE LOGIN lab_reader WITH PASSWORD=N'{{PASSWORD}}', CHECK_POLICY=ON, CHECK_EXPIRATION=OFF;
GO
USE WideWorldImporters;
IF USER_ID(N'lab_reader') IS NULL CREATE USER lab_reader FOR LOGIN lab_reader;
IF IS_ROLEMEMBER(N'db_datareader', N'lab_reader') <> 1 ALTER ROLE db_datareader ADD MEMBER lab_reader;
GRANT VIEW DEFINITION TO lab_reader;
GO
