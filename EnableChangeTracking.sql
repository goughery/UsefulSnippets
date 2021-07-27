--find which tables have change tracking
--select b.name, a.* from sys.tables b
--inner join sys.change_tracking_tables a
--on a.object_id = b.object_id
--order by b.name


--Gets tables from sys.tables and iterates through, enabling change tracking

-- ALTER TABLE dbo.dfnd_savecancel
-- ENABLE CHANGE_TRACKING  
-- WITH (TRACK_COLUMNS_UPDATED = ON) 



DECLARE @UserId NVARCHAR(100);
DECLARE @SqlText NVARCHAR(MAX);
DECLARE @RowCount int;
DECLARE @CurrentIndex int = 0;
DECLARE @SqlLine NVARCHAR(MAX);

SET @UserId = 'Prd_BIAzureKV.sql';

DECLARE db_cursor CURSOR FOR 
select 
'
IF NOT EXISTS (SELECT object_id FROM sys.change_Tracking_tables WHERE object_id = ' + CAST(object_id as varchar(50)) + ')
BEGIN
ALTER TABLE [' + SCHEMA_NAME(schema_id) + '].[' + [name] + '] ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = ON); 
GRANT VIEW CHANGE TRACKING ON [' + SCHEMA_NAME(schema_id) + '].[' + [name] + '] TO [' + @UserId + ']; 
PRINT ''Change Tracking Enabled on [' + SCHEMA_NAME(schema_id) + '].[' + [name] + ']''; 
END
ELSE
BEGIN
	PRINT ''Change Tracking already enabled for [' + SCHEMA_NAME(schema_id) + '].[' + [name] + ']''; 
END
' as SqlText
 FROM sys.tables
 ORDER BY [name]

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @SqlText  

WHILE @@FETCH_STATUS = 0  
BEGIN  
      EXEC sp_executesql @SqlText

      FETCH NEXT FROM db_cursor INTO @SqlText 
END 

CLOSE db_cursor  
DEALLOCATE db_cursor 
