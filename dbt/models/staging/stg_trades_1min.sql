-- Clean the raw candles: readable coin name, remove any duplicates.
with source as (
    select * from {{ source('crypto_raw', 'trades_1min') }}
),

deduplicated as (
    -- A Spark restart can occasionally write a window twice; keep one.
    select
        *,
        row_number() over (partition by symbol, window_start order by window_end) as row_num
    from source
)

select
    symbol + '_' + convert(varchar(16), window_start, 120)   as candle_id,  -- e.g. BTCUSDT_2026-10-25 14:03
    symbol,
    replace(symbol, 'USDT', '')                              as coin,
    window_start,
    window_end,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_volume                                             as volume_usdt,
    trade_count,
    buy_quote_volume                                         as buy_volume_usdt,
    sell_quote_volume                                        as sell_volume_usdt
from deduplicated
where row_num = 1
