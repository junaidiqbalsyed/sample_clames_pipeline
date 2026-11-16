# sql/

T-SQL for the Azure SQL source database, run in this order:

1. `01_create_tables.sql` - schema (also run by `src/smart_clames/database/load_sql_server.py`; **drops existing tables**)
2. `02_enable_change_tracking.sql` - Change Tracking + CDC, required by Lakeflow Connect
3. `03_lakeflow_utility_objects.sql` - official Databricks utility script v1.7 (unmodified); then run
   `EXEC dbo.lakeflowSetupChangeTracking @Tables = NULL, @User = '<sql user>', @CreateDdlSupportingObjects = 1;`
