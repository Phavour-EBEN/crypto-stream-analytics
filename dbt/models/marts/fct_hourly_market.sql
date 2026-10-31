with candles as (
    select
        *,
        -- round down to the hour (T-SQL has no timestamp_trunc)
        cast(dateadd(hour, datediff(hour, 0, window_start), 0) as datetime2(0)) as hour_start
    from {{ ref('stg_trades_1min') }}
),

ranked as (
    select
        *,
        row_number() over (partition by coin, hour_start order by window_start asc)  as rn_first,
        row_number() over (partition by coin, hour_start order by window_start desc) as rn_last
    from candles
)

select
    coin,
    hour_start,
    max(case when rn_first = 1 then open_price end)   as open_price,   -- first minute's open
    max(high_price)                                   as high_price,
    min(low_price)                                    as low_price,
    max(case when rn_last = 1 then close_price end)   as close_price,  -- last minute's close
    sum(volume_usdt)                                  as volume_usdt,
    sum(trade_count)                                  as trade_count,
    sum(buy_volume_usdt)                              as buy_volume_usdt,
    sum(sell_volume_usdt)                             as sell_volume_usdt
from ranked
group by coin, hour_start
