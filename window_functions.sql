SELECT 
	bar, 
	max(bar) OVER () -- empty window clause will use everything
FROM foo;



SELECT symbol, 
	max(volume) 
FROM trades 
WHERE time >= '2021-11-29 09:00:00' AND time <= '2021-11-29 12:00:00'
GROUP BY symbol;

SELECT *,
	max(volume) OVER (PARTITION BY symbol), 
	min(volume) OVER (PARTITION BY symbol), 
	max(price) OVER (PARTITION BY symbol)
FROM trades 
WHERE time >= '2021-11-29 09:00:00' AND time <= '2021-11-29 12:00:00'
ORDER BY time DESC, volume ASC
LIMIT 100;


SELECT *,
	max(volume) OVER each_symbol AS max_vol_by_symbol, 
	min(volume) OVER each_symbol AS min_vol_by_symbol, 
	max(price) OVER each_symbol AS max_price_by_symbol,
	max(volume) OVER everything AS max_vol_overall
FROM trades 
WHERE time >= '2021-11-29 09:00:00' AND time <= '2021-11-29 12:00:00'
WINDOW 
	each_symbol AS (PARTITION BY symbol),
	everything AS () 
ORDER BY time DESC, volume ASC
LIMIT 100;

SELECT *, 
	sum(volume) OVER cumulative_by_symbol AS cumulative_volume_by_symbol,
	sum(volume) OVER each_symbol AS total_volume_by_symbol,
	sum(volume) OVER everything AS overall_total_trading_volume
	
FROM trades 
WHERE time >= '2021-11-29 09:00:00' AND time <= '2021-11-29 12:00:00'
WINDOW 
	cumulative_by_symbol AS (PARTITION BY symbol ORDER BY time ASC, price, volume), 
	each_symbol AS (PARTITION BY symbol),
	everything AS () ;
	
	
WITH windows AS (SELECT *, 
	max(time) OVER next_30 AS max_time_next_30,
	sum(stats_agg(volume) OVER next_30) AS total_volume_next_30,
	sum(volume) OVER cumulative_by_symbol AS cumulative_volume_by_symbol,
	sum(volume) OVER each_symbol AS total_volume_by_symbol,
	sum(volume) OVER everything AS overall_total_trading_volume
	
FROM trades 
WHERE time >= '2021-11-29 09:00:00' AND time <= '2021-11-29 12:30:00'
WINDOW 
	next_30 AS (PARTITION BY symbol ORDER BY time ASC RANGE BETWEEN CURRENT ROW AND '30 min' FOLLOWING),
	cumulative_by_symbol AS (PARTITION BY symbol ORDER BY time ASC, price, volume), 
	each_symbol AS (PARTITION BY symbol),
	everything AS ())
SELECT * 
FROM windows
WHERE time <= '2021-11-29 12:00:00'
;