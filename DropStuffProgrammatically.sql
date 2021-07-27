--drop views

USE REITDW_Demo

Declare @viewName varchar(500) 
Declare cur Cursor For Select obj.[name] 
From sys.objects obj
INNER JOIN sys.schemas sc
ON sc.schema_id = obj.schema_id
where type = 'v'
AND
sc.name = 'dbo'

Open cur 
Fetch Next From cur Into @viewName 
While @@fetch_status = 0 
Begin 
 Exec('drop view ' + @viewName) 
 Fetch Next From cur Into @viewName 
End
Close cur 
Deallocate cur

--drop sprocs

USE REITDW_Demo

Declare @procName varchar(500) 
Declare cur Cursor For Select obj.[name] 
From sys.objects obj
INNER JOIN sys.schemas sc
ON sc.schema_id = obj.schema_id
where type = 'p'
AND
sc.name = 'dbo'

Open cur 
Fetch Next From cur Into @procName 
While @@fetch_status = 0 
Begin 
 Exec('drop procedure ' + @procName) 
 Fetch Next From cur Into @procName 
End
Close cur 
Deallocate cur


--drop functions

USE REITDW_Demo

Declare @funcName varchar(500) 
Declare cur Cursor For Select obj.[name] 
From sys.objects obj
INNER JOIN sys.schemas sc
ON sc.schema_id = obj.schema_id
where type IN (N'FN', N'IF', N'TF', N'FS', N'FT')
AND
sc.name = 'dbo'

Open cur 
Fetch Next From cur Into @funcName 
While @@fetch_status = 0 
Begin 
 Exec('drop function ' + @funcName) 
 Fetch Next From cur Into @funcName
End
Close cur 
Deallocate cur
