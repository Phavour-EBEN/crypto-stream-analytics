-- Makes Spark's Parquet files in the data lake queryable from Synapse serverless.
-- Run with sqlcmd, which fills in $(STORAGE_ACCOUNT) and $(MASTER_KEY_PASSWORD).
-- In Synapse Studio, replace both by hand first. Safe to run again.

-- 1. A master key encrypts the credential below. Needed once per database.
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(MASTER_KEY_PASSWORD)';
GO

-- 2. Read the lake as Synapse's managed identity (it has Storage Blob Data Contributor).
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'synapse_identity')
    CREATE DATABASE SCOPED CREDENTIAL synapse_identity WITH IDENTITY = 'Managed Identity';
GO

-- 3. Shortcut to the lake container
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'crypto_lake')
    CREATE EXTERNAL DATA SOURCE crypto_lake WITH (
        LOCATION   = 'https://$(STORAGE_ACCOUNT).dfs.core.windows.net/lake',
        CREDENTIAL = synapse_identity
    );
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'crypto_raw')
    EXEC('CREATE SCHEMA crypto_raw');
GO

-- 4. The "table": a view over every day's Parquet files.
--    trade_date=*/*.parquet skips Spark's _spark_metadata folder.
--    trade_date comes from the folder name, so filtering on it skips whole days.
IF OBJECT_ID('crypto_raw.trades_1min') IS NOT NULL
    DROP VIEW crypto_raw.trades_1min;
GO
CREATE VIEW crypto_raw.trades_1min AS
SELECT
    CAST(r.filepath(1) AS DATE) AS trade_date,
    r.*
FROM OPENROWSET(
    BULK 'trades_1min/trade_date=*/*.parquet',
    DATA_SOURCE = 'crypto_lake',
    FORMAT = 'PARQUET'
)
WITH (
    window_start       DATETIME2,
    window_end         DATETIME2,
    symbol             VARCHAR(20),
    open_price         FLOAT,
    high_price         FLOAT,
    low_price          FLOAT,
    close_price        FLOAT,
    volume             FLOAT,
    quote_volume       FLOAT,
    trade_count        BIGINT,
    buy_quote_volume   FLOAT,
    sell_quote_volume  FLOAT
) AS r;
GO
