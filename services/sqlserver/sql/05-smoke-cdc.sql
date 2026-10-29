SET NOCOUNT ON;
SET IMPLICIT_TRANSACTIONS OFF;
USE WideWorldImporters;
GO
DECLARE @id uniqueidentifier = NEWID();
INSERT INTO lab.CdcProbe (ProbeID, Payload) VALUES (@id, N'before');
UPDATE lab.CdcProbe SET Payload = N'after' WHERE ProbeID = @id;
DELETE FROM lab.CdcProbe WHERE ProbeID = @id;

DECLARE @deadline datetime2 = DATEADD(second, 60, SYSUTCDATETIME());
WHILE (SELECT COUNT(*) FROM cdc.lab_CdcProbe_CT WHERE ProbeID = @id) < 4
      AND SYSUTCDATETIME() < @deadline
    WAITFOR DELAY '00:00:01';

IF (SELECT COUNT(DISTINCT __$operation) FROM cdc.lab_CdcProbe_CT WHERE ProbeID = @id) <> 4
    THROW 51007, 'CDC did not capture insert, update before/after and delete within 60 seconds.', 1;

SELECT __$operation AS Operation, Payload, CONVERT(varchar(22), sys.fn_varbintohexstr(__$start_lsn)) AS CommitLSN
FROM cdc.lab_CdcProbe_CT WHERE ProbeID = @id
ORDER BY __$start_lsn, __$seqval, __$operation;
PRINT N'CDC smoke test passed: insert, update and delete captured.';
GO
