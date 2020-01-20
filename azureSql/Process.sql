CREATE PROCEDURE stage.spRawToProcess
AS
BEGIN
	-- Find boundaries
	DECLARE @xOffset INT
	DECLARE @yOffset INT
	DECLARE @rangeEasting INT
	DECLARE @rangeNorthing INT
	DECLARE @xStorageSize INT = 1000
	DECLARE @yStorageSize INT = 2000
	DECLARE @xStorageRatio FLOAT
	DECLARE @yStorageRatio FLOAT

	;WITH a AS (
	SELECT MIN(Longitude) AS minLong, MIN(Latitude) AS minLat,
	MAX(Longitude) AS maxLong, MAX(Latitude) AS maxLat
	FROM stage.rawPostcodes
	)
	SELECT @xOffset = (minLong + 180) * 10000,  
		   @yOffset = (minLat) * 10000,        
		   @rangeEasting = (maxLong + 180) * 10000 - (minLong + 180) * 10000,
		   @rangeNorthing = (maxLat) * 10000 - (minLat) * 10000 
	FROM a

	SELECT @xStorageRatio = CAST(@rangeEasting AS FLOAT) / CAST(@xStorageSize AS FLOAT)
	SELECT @yStorageRatio = CAST(@rangeNorthing AS FLOAT) / CAST(@yStorageSize AS FLOAT)

	print @xOffset
	print @yOffset
	print @rangeEasting
	print @rangeNorthing
	print @xStorageRatio
	print @yStorageRatio


	TRUNCATE TABLE process.xy

	-- Read raw CSV into xy table
	;WITH a AS (
	SELECT SUBSTRING(Postcode,1, CHARINDEX(' ',Postcode)) AS Postcode, --extract district
		   Longitude, Latitude,
		   CAST((((Longitude + 180) * 10000) - @xOffset) / @xStorageRatio AS INT) AS x,
		   CAST((((Latitude) * 10000) - @yOffset) / @yStorageRatio AS INT) AS y
	FROM stage.rawPostcodes
	)
	INSERT INTO process.xy (Postcode, x, y, [Count])
	SELECT Postcode, x, y, COUNT(1) AS [Count]
	FROM a
	GROUP BY Postcode, x,y
	ORDER BY Postcode, x,y

END
GO


CREATE PROCEDURE process.spGrowProcessed
AS
BEGIN
	-- Grow
	DECLARE @distance INT = 20
	DECLARE @infils INT
	DECLARE @xStorageSize INT = 1000
	DECLARE @yStorageSize INT = 2000
	DECLARE @numberOfIterations INT = 5

	-- expand loop
	--   set up src table
	DROP TABLE IF EXISTS #srcTab
	SELECT postcode, x, y, [Count]
	INTO #srcTab
	FROM process.xy

	DECLARE @cnt INT = 0

	-- Loop through data set growing points
	WHILE @cnt < @numberOfIterations
	BEGIN
		DECLARE @i VARCHAR(1000) = 'Iteration: ' + CAST(@cnt AS VARCHAR)
		RAISERROR(@i,0,1) WITH NOWAIT

		-- expand x left and y up
		--   find last point for growth, left and up
		DROP TABLE IF EXISTS #expand1
		;WITH a AS (
			SELECT postcode, x,y, [count],
				LAG(x) OVER (PARTITION BY y,postcode ORDER BY x ASC) AS xSamePC,
				LAG(x) OVER (PARTITION BY y ORDER BY x ASC) AS xDiffPC,
				LAG(y) OVER (PARTITION BY x,postcode ORDER BY y ASC) AS ySamePC,
				LAG(y) OVER (PARTITION BY x ORDER BY y ASC) AS yDiffPC
			FROM #srcTab
		) 
		SELECT postcode, x, y, [count], MAX(xSamePC) AS xSamePC, 
		MAX(xDiffPC) AS xDiffPC, MAX(ySamePC) AS ySamePC, MAX(yDiffPC) AS yDiffPC
		INTO #expand1
		FROM a
		GROUP BY postcode, x, y, [count]

		SELECT @i  = '   Wrote #expand1'
		RAISERROR(@i,0,1) WITH NOWAIT

		-- create infil table
		DECLARE @xyMax INT 
		SELECT @xyMax = IIF(@xStorageSize > @yStorageSize, @xStorageSize, @yStorageSize)
		DROP TABLE IF EXISTS #countSet
		;WITH a AS (
			SELECT 0 AS [Count] 
			UNION ALL 
			SELECT [Count]+1 AS [Count] FROM a
			WHERE [Count] < @xyMax
		)
		SELECT * 
		INTO #countSet
		FROM  a
		OPTION (MAXRECURSION 0)

		--  self join to fill gaps in x (then y)  => THIS only does same PC, not different. LIMITATION
		DROP TABLE IF EXISTS #expand2
		;WITH a AS (
		SELECT 
			postcode, cs.[Count] AS x, y, IIF(x=cs.[Count], e1.[Count], 1) AS [Count]
		FROM #expand1 e1
		INNER JOIN #countSet cs
		ON  e1.x >= cs.[Count]
		AND e1.xSamePC <= cs.[Count]
		) 
		SELECT postcode, x, y, MAX([Count]) AS [Count]
		INTO #expand2
		FROM a
		GROUP BY postcode, y,x

		SELECT @i  = '   Wrote #expand2 1/2'
		RAISERROR(@i,0,1) WITH NOWAIT

		;WITH a AS (
		SELECT 
			postcode, cs.[Count] AS x, y, IIF(y=cs.[Count], e1.[Count], 1) AS [Count]
		FROM #expand1 e1
		INNER JOIN #countSet cs
		ON  e1.y >= cs.[Count]
		AND e1.ySamePC <= cs.[Count]
		) 
		INSERT INTO #expand2
		SELECT postcode, x, y, MAX([Count]) AS [Count]
		FROM a
		GROUP BY postcode, y,x

		SELECT @i  = '   Wrote #expand2 2/2'
		RAISERROR(@i,0,1) WITH NOWAIT


		-- remove dupes
		DROP TABLE IF EXISTS #expand3
		SELECT postcode, x, y, MAX([Count]) AS [Count]
		INTO #expand3
		FROM #expand2
		GROUP BY postcode, x, y

		SELECT @i  = '   Wrote #expand3'
		RAISERROR(@i,0,1) WITH NOWAIT


		SELECT COUNT(*) AS newRows FROM #expand3
		SELECT COUNT(*) AS oldRows FROM #srcTab

		TRUNCATE TABLE #srcTab
		INSERT INTO #srcTab
		SELECT postcode, x, y, [Count]
		FROM #expand3

		SELECT @cnt += 1
	END

	-- Save final grown DB
	truncate table process.xyFinal
	insert into process.xyFinal
	select postcode, x,y,[Count]
	from #srcTab

END
GO


CREATE PROCEDURE img.spGenerateImage
AS
BEGIN
	-- GENERATE IMAGE
	DECLARE @xStorageSize INT = 1000
	DECLARE @yStorageSize INT = 2000

	DROP TABLE IF EXISTS #srcTab
	SELECT postcode, x,y,[Count]
	INTO #srcTab
	FROM process.xy -- use ungrown points
	--FROM process.xyFinal -- use grown points. UNCOMMENT AND COMMENT ABOVE

	TRUNCATE TABLE img.images 
	INSERT INTO img.images ([byteCount],b) VALUES 
	(0,0x42),(1,0x4D),
	(2,0x00),(3,0x00),(4,0x00),(5,0x00),  -- file size, needs to be regenerated
	(6,0x00),(7,0x00),
	(8,0x00),(9,0x00),
	(10,0x3e),(11,0x00),(12,0x00),(13,0x00),

	(14,0x28),(15,0x00),(16,0x00),(17,0x00),    -- already reversed for LE
	(18,0xE8),(19,0x03),(20,0x00),(21,0x00),    -- 1000 in LE
	(22,0xD0),(23,0x07),(24,0x00),(25,0x00),    -- 2000 in LE
	(26,0x01),(27,0x00),              -- 1 plane
	(28,0x01),(29,0x00),              -- 1 bit per pixel
	(30,0x00),(31,0x00),(32,0x00),(33,0x00),    -- no compression
	(34,0x70),(35,0x01),(36,0x00),(37,0x00),    -- 0 for no compression
	(38,0x25),(39,0x16),(40,0x00),(41,0x00),    -- pixel per meter x
	(42,0x25),(43,0x16),(44,0x00),(45,0x00),    -- pixel per meter y
	(46,0x00),(47,0x00),(48,0x00),(49,0x00),    -- 0 to use 1 bit per pixel value
	(50,0x00),(51,0x00),(52,0x00),(53,0x00),    -- ignored


	--colour table
	(54,0x00),(55,0x00),(56,0x00),(57,0x00),
	(58,0xff),(59,0xff),(60,0xff),(61,0x00)



	-- create infil table
	DECLARE @xyMax INT 
	SELECT @xyMax = IIF(@xStorageSize > @yStorageSize, @xStorageSize, @yStorageSize)
	DROP TABLE IF EXISTS #countSet
	;WITH a AS (
		SELECT 0 AS [Count] 
		UNION ALL 
		SELECT [Count]+1 AS [Count] FROM a
		WHERE [Count] < @xyMax
	)
	SELECT * 
	INTO #countSet
	FROM  a
	OPTION (MAXRECURSION 0)

	DECLARE @x INT, @y INT, @bit INT, @msg VARCHAR(1000)
	--declare @y int = 500

	SELECT @y = @yStorageSize

	DECLARE @byteCounter INT = 0
	SELECT @ByteCounter = MAX(byteCount) + 1 FROM img.images

	--SELECT @ByteCounter = 0

	DECLARE @bitPad INT = (32 - @xStorageSize % 32) % 32
	DECLARE @bytePad INT = @bitPad / 8

	DROP TABLE IF EXISTS #xyFull
	SELECT cx.[Count] AS x, cy.[Count] AS y
	INTO #xyFull
	FROM #countSet cx
	CROSS JOIN #countSet cy
	WHERE cx.[Count] < @xStorageSize + @bitPad -- add padding to 4 byte boundary
	AND   cy.[Count] < @yStorageSize


	;with a as (
			select xy.x,xy.y,
			iif(MAX(s.[Count])>0,1,0) AS bitn
			from #srcTab s
			RIGHT JOIN #xyFull xy
			ON  s.x = xy.x
			AND s.y = xy.y
			GROUP BY xy.x,xy.y
	), b AS (
		SELECT x,y,
			bitn as bit0,
			LEAD(bitn,1,0) OVER (PARTITION BY y ORDER BY x) AS bit1,
			LEAD(bitn,2,0) OVER (PARTITION BY y ORDER BY x) AS bit2,
			LEAD(bitn,3,0) OVER (PARTITION BY y ORDER BY x) AS bit3,
			LEAD(bitn,4,0) OVER (PARTITION BY y ORDER BY x) AS bit4,
			LEAD(bitn,5,0) OVER (PARTITION BY y ORDER BY x) AS bit5,
			LEAD(bitn,6,0) OVER (PARTITION BY y ORDER BY x) AS bit6,
			LEAD(bitn,7,0) OVER (PARTITION BY y ORDER BY x) AS bit7
		FROM a
	)
	INSERT INTO img.images ([byteCount],b,x,y)
	SELECT
			y * (@xStorageSize / 8 + @bytePad) + x/8 + @ByteCounter,
			CAST(bit0 + bit1 * 2 + bit2 * 4 + bit3 * 8 + bit4 * 16 + bit5 * 32 + bit6 * 64 + bit7 * 128 AS BINARY(1)) bb,
			x,y
	FROM b 
	WHERE x % 8 = 0

	
	-- Update file size
	--DECLARE @ByteCounter INT
	SELECT @ByteCounter = MAX(byteCount) + 1 FROM img.images

	DECLARE @bc1 INT, @bc2 INT, @bc3 INT, @bc4 INT
	SELECT @bc1 = @byteCounter % 256,
		   @bc2 = @byteCounter / 256 % 256,
		   @bc3 = @byteCounter / 256 / 256 % 256,
		   @bc4 = @byteCounter / 256 / 256 / 256 % 256
	   		 
	UPDATE a SET b = @bc1 FROM img.images a WHERE byteCount = 2
	UPDATE a SET b = @bc2 FROM img.images a WHERE byteCount = 3
	UPDATE a SET b = @bc3 FROM img.images a WHERE byteCount = 4
	UPDATE a SET b = @bc4 FROM img.images a WHERE byteCount = 5

END

GO