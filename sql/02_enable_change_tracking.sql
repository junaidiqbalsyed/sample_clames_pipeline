-- Enable Change Tracking and/or CDC on the smart-claims source tables (Azure SQL Database).
-- Run in the Azure portal Query editor or any SQL client connected to database [jd-sql-learn-2026].
-- Lakeflow Connect needs one of the two: Change Tracking is the lighter option, CDC keeps full change history.
-- Both are enabled below to match your template; skip section 3/4 if you only want Change Tracking.
-- Re-run this after load_sql_server.py: it drops and recreates the tables, which removes these settings.

-- 1. ENABLE CHANGE TRACKING ON THE DATABASE --
ALTER DATABASE [jd-sql-learn-2026]
SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);
GO

-- 2. ENABLE CHANGE TRACKING ON THE TABLES (each has a primary key, which this requires) --
ALTER TABLE dbo.policies  ENABLE CHANGE_TRACKING;
ALTER TABLE dbo.claims    ENABLE CHANGE_TRACKING;
ALTER TABLE dbo.customers ENABLE CHANGE_TRACKING;
GO

-- 3. ENABLE CDC ON THE DATABASE (Azure SQL: no msdb.dbo.rds_cdc_enable_db, that is AWS RDS only) --
EXEC sys.sp_cdc_enable_db;
GO

-- 4. ENABLE CDC ON THE TABLES --
EXEC sys.sp_cdc_enable_table
    @source_schema        = N'dbo',
    @source_name          = N'policies',
    @role_name            = NULL,
    @supports_net_changes = 1;

EXEC sys.sp_cdc_enable_table
    @source_schema        = N'dbo',
    @source_name          = N'claims',
    @role_name            = NULL,
    @supports_net_changes = 1;

EXEC sys.sp_cdc_enable_table
    @source_schema        = N'dbo',
    @source_name          = N'customers',
    @role_name            = NULL,
    @supports_net_changes = 1;
GO

-- 5. VERIFY --
SELECT DB_NAME(database_id) AS db, retention_period, retention_period_units_desc, is_auto_cleanup_on
FROM sys.change_tracking_databases;

SELECT OBJECT_NAME(object_id) AS table_name, 'CHANGE_TRACKING' AS feature FROM sys.change_tracking_tables
UNION ALL
SELECT name, 'CDC' FROM sys.tables WHERE is_tracked_by_cdc = 1;
