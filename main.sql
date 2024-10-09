/*
drop table if exists dbo.orders;
create table dbo.orders(
	id serial primary key, 
	user_id varchar,
	order_id varchar,
	order_time integer,
	order_cost real,
	success_order_flg bool
);


INSERT INTO dbo.orders (user_id, order_id, order_time, order_cost, success_order_flg)
SELECT 
    'user_' || FLOOR(RANDOM() * 1000)::TEXT AS user_id,
    'order_' || FLOOR(RANDOM() * 10000)::TEXT AS order_id,
   	EXTRACT(EPOCH FROM TIMESTAMP '2023-01-01' + RANDOM() * (CURRENT_TIMESTAMP - TIMESTAMP '2023-01-01'))::BIGINT AS order_time,
    RANDOM() * (1000 - 10) + 10 AS order_cost,
    CASE WHEN RANDOM() > 0.3 THEN TRUE ELSE FALSE END AS success_order_flg
FROM 
    generate_series(1, 1000);

*/
drop table if exists dbo.analytics;

WITH first_orders AS (
    SELECT 
    	user_id, 
    	order_cost,
    	to_timestamp(order_time)::date AS first_order_time,
    	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time) AS rn
    FROM dbo.orders
),
middleware_analytics as (
	SELECT 
	    to_timestamp(o.order_time)::date AS date,
	    SUM(fo.order_cost) AS gmv360d_new,
	    0 AS gmv360d_reactivated,
	    COUNT(fo.user_id) AS users_count_new,
	    0 AS users_count_reactivated,
	    row_number () over (partition by to_timestamp(o.order_time)::date order by to_timestamp(o.order_time)) as rn
	FROM dbo.orders o
	LEFT JOIN first_orders fo 
	    ON fo.first_order_time = to_timestamp(o.order_time)::date
	where fo.rn = 1
	GROUP BY to_timestamp(o.order_time)
	ORDER BY to_timestamp(o.order_time)ASC
)

--select * from first_orders where first_order_time = '2023-01-03' order by user_id, first_order_time
--select * from middleware_analytics --where date = '2024-01-11'
SELECT 
    a.date,
    a.gmv360d_new,
    a.gmv360d_reactivated,
    a.users_count_new,
    a.users_count_reactivated
into dbo.analytics
FROM middleware_analytics a
where a.rn = 1
order by date;

select * from dbo.analytics a ;


-- select user_id, count(user_id) from dbo.orders group by user_id having user_id = 'user_740' order by count(user_id) desc
-- select user_id, order_cost from dbo.orders o order by user_id