drop table if exists dbo.orders;
create table dbo.orders(
	id serial primary key, 
	user_id varchar,
	order_id varchar,
	order_time integer,
	order_cost real,
	success_order_flg bool
);
/*
drop table if exists dbo.analytics;
create table dbo.analytics(
	id serial primary key,
	date date,
	gmv360d_new real,
	gmv360d_reactivated real,
	users_count_new integer,
	users_count_reactivated integer
);
*/


INSERT INTO dbo.orders (user_id, order_id, order_time, order_cost, success_order_flg)
SELECT 
    'user_' || FLOOR(RANDOM() * 1000)::TEXT AS user_id,
    'order_' || FLOOR(RANDOM() * 10000)::TEXT AS order_id,
   	EXTRACT(EPOCH FROM TIMESTAMP '2023-01-01' + RANDOM() * (CURRENT_TIMESTAMP - TIMESTAMP '2023-01-01'))::BIGINT AS order_time,
    RANDOM() * (1000 - 10) + 10 AS order_cost,
    CASE WHEN RANDOM() > 0.3 THEN TRUE ELSE FALSE END AS success_order_flg
FROM 
    generate_series(1, 1000);


drop table if exists analytics;

WITH first_orders AS (
    SELECT user_id, MIN(to_timestamp(order_time)) AS first_order_time
    FROM dbo.orders
    GROUP BY user_id
)

SELECT 
    to_timestamp(o.order_time)::date AS date,
    COUNT(fo.user_id) AS gmv360d_new,
    0 AS gmv360d_reactivated,
    0 AS users_count_new,
    0 AS users_count_reactivated
INTO dbo.analytics 
FROM dbo.orders o
LEFT JOIN first_orders fo 
    ON fo.first_order_time = to_timestamp(o.order_time)
GROUP BY to_timestamp(o.order_time)
ORDER BY to_timestamp(o.order_time) ASC;


SELECT 
    date,
    SUM(gmv360d_new) AS gmv360d_new,
    SUM(gmv360d_reactivated) AS gmv360d_reactivated,
    SUM(users_count_new) AS users_count_new,
    SUM(users_count_reactivated) AS users_count_reactivated
FROM dbo.analytics a
GROUP BY date
order by date;




-- select user_id, count(user_id) from dbo.orders group by user_id having user_id = 'user_740' order by count(user_id) desc
-- select user_id, date(to_timestamp(order_time)) from dbo.orders o order by order_time