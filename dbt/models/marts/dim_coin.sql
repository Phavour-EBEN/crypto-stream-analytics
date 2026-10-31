-- One row per coin. Power BI links both fact tables to this,
-- so a single coin filter controls every chart.
select distinct
    coin,
    symbol
from {{ ref('stg_trades_1min') }}
