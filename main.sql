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


WITH extended_orders AS (
	SELECT 
		to_timestamp(order_time)::date AS order_date,
		user_id AS user_id,
		order_id AS order_id,
		order_cost,
		success_order_flg,
		case 
			when row_number() over (partition by user_id order by order_time) > 1 then 0
			else row_number() over (partition by user_id order by order_time)
		end as is_first_order
		
	FROM 
		dbo.orders
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
	order by to_timestamp(order_time)::date 

),
yat_order as (
	select
		eo.order_date,
		eo.user_id,
		eo.order_cost,
		eo.is_first_order,
		ro.is_reactivated
	from extended_orders eo 
	left join reactivated_orders ro 
		on eo.order_date = ro.date and ro.user_id = eo.user_id
	--where eo.is_first_order = 1 and ro.is_reactivated = 0
	order by order_date
)

--select * from yat_order

select 
	order_date,
	sum(is_first_order) as first_order,
	case
		when sum(is_reactivated) is null then 0
		else sum(is_reactivated) 
	end as reactivated

from yat_order
group by order_date



--SELECT user_id, date FROM reactivated_orders where is_reactivated = 1

-- null в is_reactivated значит заказ единственный, 0 значит заказ не реактивированный, и 1 значит реактивированный

--select to_timestamp(order_time)::date from dbo.orders o where user_id = 'user_105'


/*
SELECT 
	order_date,
	count(user_id)
FROM extended_orders 
where is_first_order = 1
group by order_date
order by order_date
*/





/*
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
*/