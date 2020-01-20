To use this...

1. Configure an Azure SQL DB instance. Suggest 100+ DTU.

2. Set environment variables:
SET AZURESQLUSER=<azure sql user>
SET AZURESQLPWD=<azure sql password>
SET AZURESQLSVRURL=<azure sql server url>
SET AZURESQLDB=<azure sql db>

3. Open SSMS and run:
CreateSchema.sql to create the schema and tables needed
Process.sql to create sprocs

4. Install golang if not on system

5. Run the orchestrate go program...
go run DbOrchestrate.go
