select
    coin,
    trade_date,
    sum(volume_usdt)                                          as volume_usdt,
    sum(trade_count)                                          as trade_count,
    sum(buy_volume_usdt)                                      as buy_volume_usdt,
    sum(sell_volume_usdt)                                     as sell_volume_usdt,
    sum(buy_volume_usdt) / nullif(sum(volume_usdt), 0)        as buy_share
from {{ ref('stg_trades_1min') }}
group by coin, trade_date
