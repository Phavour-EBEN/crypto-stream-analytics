# Crypto Stream Analytics

Real-time pipeline that captures live crypto trades, aggregates them into
minute-level market data, and visualizes trading activity across coins.

Final project for the Data Engineering Zoomcamp. Work in progress.

## Data warehouse: Synapse serverless SQL

Spark writes 1-minute candles as Parquet files to Azure Data Lake Storage Gen2.
Synapse's serverless SQL pool queries those files in place: no data is loaded or
copied. [`warehouse/create_views.sql`](warehouse/create_views.sql) creates the view
`crypto_raw.trades_1min` over the files, and dbt and Power BI read from that view.
New files appear in queries automatically.

### Partitioned by `trade_date`

Spark writes one folder per day (`trades_1min/trade_date=2026-10-30/`). The view
reads `trade_date` from the folder name with `filepath(1)`, so a query that filters
on `trade_date` opens only the matching day folders (partition elimination). Every
dashboard chart filters by a date range, and serverless bills by the amount of data
read, so this makes queries both faster and cheaper.

### Why not also partition by coin

A `symbol=` folder level would let per-coin queries skip the other coins, but it
would create 8 times more files. Each one-minute batch already produces a small
file (about 8 rows, one per coin), and many small files slow serverless down more
than reading all coins of a day does. For the same reason, Spark writes one file
per batch (`coalesce(1)`) rather than one per Spark partition.

### Clustering

Serverless has no indexes, so its equivalent of clustering is how rows are laid
out in files. Each file holds the minute (or few minutes) that one batch finalized, so the files are already
clustered by time, which matches the main time-series chart. Text columns use the
`Latin1_General_100_BIN2_UTF8` collation, which lets filters on `symbol` be pushed
down into the Parquet reader.

### Why serverless instead of a dedicated SQL pool

A dedicated pool costs about $1–1.5 per hour even when idle. Serverless bills about
$5 per TB read, and this dataset is a few MB, so queries cost fractions of a cent.
At a much larger scale (billions of rows or many dashboard users), the better choice
would be loading into a dedicated pool, or compacting the small files once a day.
