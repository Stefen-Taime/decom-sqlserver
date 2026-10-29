USE WideWorldImporters;
-- WWI's existing customer predicate grants visibility through '<territory> Sales' roles.
-- Give the two lab readers all territories, preserving the policy and its function.
DECLARE @role sysname, @member sysname, @statement nvarchar(max);
DECLARE reader_roles CURSOR LOCAL FAST_FORWARD FOR
    SELECT DISTINCT sp.SalesTerritory + N' Sales', members.name
    FROM Application.StateProvinces sp
    CROSS JOIN (VALUES (N'debezium_cdc'), (N'lab_reader')) members(name)
    WHERE USER_ID(members.name) IS NOT NULL;
OPEN reader_roles;
FETCH NEXT FROM reader_roles INTO @role, @member;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF DATABASE_PRINCIPAL_ID(@role) IS NULL
    BEGIN
        SET @statement = N'CREATE ROLE ' + QUOTENAME(@role) + N';';
        EXEC sys.sp_executesql @statement;
    END;
    IF IS_ROLEMEMBER(@role, @member) <> 1
    BEGIN
        SET @statement = N'ALTER ROLE ' + QUOTENAME(@role) + N' ADD MEMBER ' + QUOTENAME(@member) + N';';
        EXEC sys.sp_executesql @statement;
    END;
    FETCH NEXT FROM reader_roles INTO @role, @member;
END;
CLOSE reader_roles;
DEALLOCATE reader_roles;
GO
