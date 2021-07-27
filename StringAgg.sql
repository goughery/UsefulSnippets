SELECT 
	STRING_AGG(
	c.name + ' ' + 
	(
		CASE WHEN (patindex('%int%', t.name) > 0 OR patindex('%money%', t.name) > 0 or patindex('%date%', t.name) > 0 or patindex('time', t.name) > 0) THEN t.name 
	ELSE
		t.name + '(' + CONVERT(varchar(10), c.max_length) + ')'
	END), ', ') WITHIN GROUP (Order BY c.column_id) AS OutputLine 
FROM    
    sys.columns c
INNER JOIN 
    sys.types t ON c.user_type_id = t.user_type_id
LEFT OUTER JOIN 
    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT OUTER JOIN 
    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
WHERE
    c.object_id = OBJECT_ID('dbo.factcustomer')
	and patindex('%created%', c.name) = 0
and patindex('%updated%', c.name) = 0