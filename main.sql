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
    ROUND(RANDOM() * (1000 - 10)) + 10 AS order_cost,
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
    	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time) AS rn_first
    FROM dbo.orders
),
reactivated_orders as (
	select 
		user_id,
		to_timestamp(order_time)::date as date,
		case 
			when to_timestamp(order_time)::date - LAG(to_timestamp(order_time)::date) over (partition by user_id order by user_id, to_timestamp(order_time)::date) 	> 90 then 1
			else 0
		end as is_reactivated

	from dbo.orders 
	where user_id in (	select user_id from dbo.orders 	group by user_id having count(user_id) >= 2)
	order by user_id

),
--select * from reactivated_orders

middleware_analytics as (
	SELECT 
	    to_timestamp(o.order_time)::date AS date,
	    SUM(fo.order_cost) AS gmv360d_new,
	    0 AS gmv360d_reactivated,
	    COUNT(fo.user_id) AS users_count_new,
	    SUM(ro.is_reactivated) AS users_count_reactivated,
	    row_number () over (partition by to_timestamp(o.order_time)::date order by to_timestamp(o.order_time)) as rn
	FROM dbo.orders o
	LEFT JOIN first_orders fo 
	    ON fo.first_order_time = to_timestamp(o.order_time)::date
	left join reactivated_orders ro
		on ro.user_id = o.user_id and ro.date = to_timestamp(o.order_time)::date
	where fo.rn_first = 1 --or fo.rn_reactivated = 1 and ro.
	GROUP BY to_timestamp(o.order_time) 
	ORDER BY to_timestamp(o.order_time) ASC
)

--select * from middleware_analytics;

--select * from first_orders where first_order_time = '2023-01-03' order by user_id, first_order_time
--select * from middleware_analytics --where date = '2024-01-11'

SELECT 
    a.date,
    max(a.gmv360d_new) as gmv360d_new,
    max(a.gmv360d_reactivated) as gmv360d_reactivated,
    max(a.users_count_new) as users_count_new,
	case 
		when max(a.users_count_reactivated) is null then 0
		else max(a.users_count_reactivated)
	end
	 as users_count_reactivated
into dbo.analytics
FROM middleware_analytics a
--where users_count_reactivated in (null)
--a.rn = 1 
group by a.date
order by date;

select * from dbo.analytics a ;


-- select user_id, count(user_id) from dbo.orders group by user_id having user_id = 'user_740' order by count(user_id) desc
-- select user_id, order_cost, to_timestamp(order_time)::date from dbo.orders o where to_timestamp(order_time)::date = '2024-10-09' order by user_id desc
-- select user_id, order_cost, to_timestamp(order_time)::date from dbo.orders o where user_id = 'user_802' order by user_id desc

/*
select 
		user_id,
		--order_cost,
		count(user_id)
		--to_timestamp(order_time)::date AS first_order_time,
		--ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time) AS rn_reactivated
from dbo.orders 
group by user_id
*/
	

--select max(c) from (select 0 as a, 5 as b, null as c)


--select date('2024-10-01') - date('2024-01-01')