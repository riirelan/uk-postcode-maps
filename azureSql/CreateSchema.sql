IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage')
BEGIN
	CREATE SCHEMA stage
END
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'process')
BEGIN
CREATE SCHEMA process
END
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'img')
BEGIN
CREATE SCHEMA img
END

GO


DROP TABLE IF EXISTS stage.rawPostcodes
CREATE TABLE stage.rawPostcodes
(
    Postcode  VARCHAR(20),
	Longitude FLOAT,
	Latitude FLOAT,
	dt DATETIME DEFAULT(GETUTCDATE())
)


DROP TABLE IF EXISTS process.xy
CREATE TABLE process.xy
(
    Postcode  VARCHAR(20),
	x         INT,
	y         INT,
	[Count]   INT
)

DROP TABLE IF EXISTS process.xyFinal
CREATE TABLE process.xyFinal
(
    Postcode  VARCHAR(20),
	x         INT,
	y         INT,
	[Count]   INT
)

DROP TABLE IF EXISTS img.images
CREATE TABLE img.images
(
	dt DATETIME DEFAULT(GETUTCDATE()),
	byteCount INT,
	b BINARY(1),
	x INT,
	y INT
)