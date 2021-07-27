DECLARE @StartDate DATE;
	DECLARE @EndDate DATE;

	SELECT @StartDate = MIN([date]) FROM (SELECT CAST(d.[date] AS date) AS [date] FROM [Stage].[gldetail] d UNION ALL SELECT CAST(dh.[date] AS date) as [date] FROM [Stage].[gldetailhist] dh) AS subquery;
	SELECT @EndDate = MAX([date]) FROM (SELECT CAST(d.[date] AS date) AS [date] FROM [Stage].[gldetail] d UNION ALL SELECT CAST(dh.[date] AS date) as [date] FROM [Stage].[gldetailhist] dh) AS subquery;

	WITH Dates AS (
	SELECT [Date] = CONVERT(DATETIME,@StartDate)

	UNION ALL 

	SELECT [Date] = DATEADD(DAY, 1, [Date])
	FROM Dates
	WHERE Date <= @EndDate
	) 
	
	
		INSERT INTO [DW].[DimDate]
	(DateKey,
	CalendarDate,
	CalendarYear,
	CalendarYearMonth,
	CalendarYearMonthName,
	CalendarMonthNumber,
	CalendarMonthName,
	CalendarMonthNameShort,
	CalendarYearQuarter,
	CalendarQuarter,
	EndOfMonth,
	GLPeriod
	)

	SELECT 

	CAST([Date] AS DATE) as DateKey,
	CAST([Date] AS DATE) as CalendarDate,
	YEAR([Date]) as CalendarYear,
	CONCAT(YEAR([Date]), MONTH([Date])) AS CalendarYearMonth,
	CONCAT(datename(month, [Date]), ' ', YEAR([Date])) AS CalendarYearMonthName,
	MONTH([Date]) as CalendarMonthNumber,
	datename(month, [Date]) AS CalendarMonthName,
	LEFT(datename(month, [Date]), 3) AS CalendarMonthNameShort,
	CASE
		WHEN DATEPART(q, [Date]) = 1 THEN CONCAT(YEAR([Date]), 'Q1')
		WHEN DATEPART(q, [Date]) = 2 THEN CONCAT(YEAR([Date]), 'Q2')
		WHEN DATEPART(q, [Date]) = 3 THEN CONCAT(YEAR([Date]), 'Q3')
		WHEN DATEPART(q, [Date]) = 4 THEN CONCAT(YEAR([Date]), 'Q4')
	END AS CalendarYearQuarter,
	CASE
		WHEN DATEPART(q, [Date]) = 1 THEN 'Q1'
		WHEN DATEPART(q, [Date]) = 2 THEN 'Q2'
		WHEN DATEPART(q, [Date]) = 3 THEN 'Q3'
		WHEN DATEPART(q, [Date]) = 4 THEN 'Q4'
	END AS CalendarQuarter,
	CASE WHEN EOMONTH([Date]) = [Date] THEN 1 ELSE 0 END as EndOfMonth,
	YEAR([Date]) * 100 + MONTH([Date]) AS GLPeriod

	FROM Dates
	OPTION (MAXRECURSION 10000)
