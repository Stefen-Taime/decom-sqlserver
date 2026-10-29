"""One isolated, temporary Orders row exercises real c/u/d CDC without changing fixture rows."""
import json
from pathlib import Path
import subprocess
import uuid

token='warehouse-pilot-'+str(uuid.uuid4())
order_id=2000000000
sql=f"""
USE WideWorldImporters;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF EXISTS(SELECT 1 FROM Sales.Orders WITH(UPDLOCK,HOLDLOCK) WHERE OrderID={order_id})
 THROW 51000,'Reserved warehouse probe key is already in use',1;
INSERT Sales.Orders(OrderID,CustomerID,SalespersonPersonID,PickedByPersonID,ContactPersonID,BackorderOrderID,
 OrderDate,ExpectedDeliveryDate,CustomerPurchaseOrderNumber,IsUndersupplyBackordered,Comments,DeliveryInstructions,
 InternalComments,PickingCompletedWhen,LastEditedBy,LastEditedWhen)
SELECT {order_id},CustomerID,SalespersonPersonID,NULL,ContactPersonID,NULL,OrderDate,ExpectedDeliveryDate,
 NULL,0,N'{token}-before',NULL,NULL,NULL,LastEditedBy,SYSUTCDATETIME()
FROM Sales.Orders WHERE OrderID=1;
IF @@ROWCOUNT<>1 THROW 51000,'Probe template row is missing',1;
UPDATE Sales.Orders SET Comments=N'{token}-after',LastEditedWhen=SYSUTCDATETIME() WHERE OrderID={order_id};
DELETE Sales.Orders WHERE OrderID={order_id};
COMMIT;
"""
subprocess.run(['docker','exec','-i','decom-sqlserver-sqlserver-1','/bin/bash','/opt/lab/bin/sqlcmd.sh'],input=sql,text=True,check=True,capture_output=True)
print(json.dumps({'order_id':order_id,'marker':token,'expected_operations':['c','u','d'],'source_row_removed_in_same_transaction':True}))
