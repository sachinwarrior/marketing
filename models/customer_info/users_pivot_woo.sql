{{ config (
    materialized="table"
)}}

-- determine what orders are subscription, what orders are upsell associating with that subscription. -> this help determines if the customers take the trial
with subscription_pivot as (select  _customer_user,
          _billing_email, post_parent, post_status, post_date_gmt, _sdc_deleted_at,
          cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_start) as date) as trial_date_start, 
          case when _schedule_trial_end != '0' then cast(_schedule_trial_end as timestamp)
          end _schedule_trial_end , 
          case when  _schedule_cancelled != '0' then cast(_schedule_cancelled as timestamp)
          end _schedule_cancelled , 
           case when _schedule_end = '0' and _schedule_next_payment !='0' then  cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_next_payment) as date) 
  when _schedule_end != '0' and _schedule_next_payment = '0' then  cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_end) as date) 
  when _schedule_end != '0' and _schedule_next_payment != '0' then cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_next_payment) as date) 
  end _schedule_next_payment,       
          from funnels_processed.subscription_pivot 
          where _billing_period = 'month' and _schedule_next_payment <>'0'), 

post_parent_table as (select subscription._customer_user, post_parent,order_id, _billing_email, post_status, order_status, post_date_gmt, 
            _schedule_trial_end, trial_date_start, _schedule_cancelled, _schedule_next_payment, 

                  from (select * from subscription_pivot where _sdc_deleted_at is null) subscription
                left join (select _customer_user, id as order_id, post_status as order_status from {{ref('transactions_dbt')}} where post_date >= '2020-10-25' and post_status in ('wc-completed','wc-processing') and products_purchased not like '%Warrior Made%') post_parent_id 
                 on subscription._customer_user= post_parent_id._customer_user and subscription.post_parent= post_parent_id.order_id
 ),
 
latest_member_status as (select B._customer_user, _billing_email, post_status as current_status, _schedule_trial_end, trial_date_start, _schedule_cancelled, _schedule_next_payment, 
from post_parent_table
INNER JOIN (
                SELECT 
                  _customer_user,
                  MAX(post_date_gmt) as date_created
                FROM
                  post_parent_table  
                GROUP BY 
                  _customer_user
          
                ) B
ON post_parent_table._customer_user = B._customer_user and B.date_created = post_parent_table.post_date_gmt),

type_customer_table as (select distinct * from (select * except(current_status), case when current_status = 'wc-pending-cancel' or current_status = 'wc-cancelled' then 'cancelled'
  when current_status = 'wc-active' then 'active'
  when current_status = 'wc-on-hold' then 'on-hold'
  end current_status, 
  case when current_status = 'wc-expired' then 'ecommerce to trial'
  when current_status = 'wc-on-hold' then 'trial to subscription'
  when current_status = 'wc-active' and _schedule_trial_end >= cast(CURRENT_TIMESTAMP() AS timestamp)  THEN 'ecommerce to trial'
  when current_status = 'wc-active' and  _schedule_trial_end < cast(CURRENT_TIMESTAMP() AS timestamp)  THEN 'trial to subscription'
  end type_customers
from latest_member_status)), 

subscription_transaction as (select _customer_user, _billing_email, max(_paid_date) as subscription_paid_date from {{ref('transactions_dbt')}} where post_date >= '2020-11-01' and post_status in ('wc-completed','wc-processing') and products_purchased like '%Warrior Made%' group by _customer_user, _billing_email order by _customer_user desc),

type_customer_2 as (select type_customer_table.* except(type_customers),
  case when type_customers is null and subscription_paid_date is not null then 'trial to subscription'
   when type_customers = 'trial to subscription' and subscription_paid_date is null then 'ecommerce to trial'
   when type_customers is null and subscription_paid_date is null then 'ecommerce only'
   else type_customers
   end type_customers 
from type_customer_table
left join subscription_transaction 
on type_customer_table._customer_user= subscription_transaction._customer_user),


-- grouping all the transactions by user_id, creating ecommerce_revenue, and subscription_revenue
transaction_group_user_id as ((select sales._customer_user, 
    complete_sales.* except(_customer_user), 
    case when total_refunded is null then 0 
    else total_refunded
    end total_refunded
  FROM (select _customer_user from {{ref('transactions_dbt')}} where post_date >= '2020-11-01') sales 

  left join (select _customer_user, sum(_order_total) as total_amount, SUM(members_revenue) as member_revenue, sum(ecommerce_revenue) as ecommerce_revenue, count(order_id) as number_orders,
     string_agg(distinct(cast(order_id as string)), ',') as full_order_id, 
      sum(cast(_order_shipping as numeric)) as order_shipping, 
      sum(cast(_order_tax as numeric)) as order_tax, 
      string_agg(distinct(products_purchased),',') as products_purchased, 
      from  (select _customer_user, cast(_order_total as numeric) as _order_total,
      _order_shipping,_order_tax, products_purchased, 
      case when products_purchased like '%Warrior Made Tribe%' then cast(_order_total as numeric) else 0 end members_revenue, 
      case when products_purchased not like '%Warrior Made Tribe%' then cast(_order_total as numeric) else 0 end ecommerce_revenue, 
      order_id,
      from {{ref('transactions_dbt')}}
      where _paid_date is not null and post_date >= '2020-11-01' and post_status  in ('wc-processing', 'wc-completed'))
      group by _customer_user) complete_sales
  on sales._customer_user = complete_sales._customer_user 
  left join (select _customer_user, sum(cast(_order_total as numeric)) as total_refunded 
          from {{ref('transactions_dbt')}}
          where _paid_date is not null and post_date >= '2020-11-01' and post_status = 'wc-refunded'
          group by _customer_user) refund_sales
   on sales._customer_user = refund_sales._customer_user
)), 
      

first_purchase_info as (
    SELECT 
      a._customer_user, 
      a.order_id as first_product_id,
      a.mktg_custom_1 as first_mktg_custom_1, 
      a.mktg_custom_2 as first_mktg_custom_2,
      a.mktg_affiliate as first_aff_id
    FROM 
          {{ref('transactions_dbt')}}  A
    INNER JOIN (
                SELECT 
                  _customer_user, 
                  MIN(post_date) as date_created
                FROM
                 {{ref('transactions_dbt')}}
                WHERE post_date >= '2020-11-01' and post_status in ('wc-completed', 'wc-processing','wc-refunded')
                GROUP BY 
                  _customer_user 
                ) B
    ON A._customer_user = B._customer_user AND A.post_date = B.date_created
  ), 
latest_purchase_info as (
    SELECT 
      a._customer_user, 
      a.order_id as latest_product_id,
      a.mktg_custom_1 as latest_mktg_custom_1, 
      a.mktg_custom_2 as latest_mktg_custom_2,
      a.mktg_affiliate as latest_aff_id
    FROM 
          {{ref('transactions_dbt')}}  A
    INNER JOIN (
                SELECT 
                  _customer_user, 
                  MAX(post_date) as date_created
                FROM
                  {{ref('transactions_dbt')}}
                WHERE post_date >= '2020-11-01' and post_status  in ('wc-completed', 'wc-processing','wc-refunded')
                GROUP BY 
                  _customer_user 
                ) B
    ON A._customer_user = B._customer_user AND A.post_date = B.date_created
  ), 
billing_cycles_table as (select complete_sales_table._customer_user, 
    case when refund_sales is not null then complete_sales - refund_sales 
    else complete_sales 
    end billing_cycles
    from
    (select _customer_user, count(order_id) as complete_sales from {{ref('transactions_dbt')}} where products_purchased like '%Warrior Made Tribe%' and  post_date >= '2020-11-01' and post_status = 'wc-completed' group by _customer_user ) complete_sales_table
left join (select _customer_user, count(order_id) as refund_sales from {{ref('transactions_dbt')}} where products_purchased like '%Warrior Made Tribe%' and  post_date >= '2020-11-01' and post_status like '%wc-refund%' group by _customer_user ) refund_sales_table
on complete_sales_table._customer_user = refund_sales_table._customer_user), 

users_pivot as (select user_tracking_id, user_email, user_id, user_registered, 
                first_name,last_name, billing_phone,
                shipping_city, shipping_state, shipping_postcode, shipping_country,
                billing_city, billing_state, billing_postcode, billing_country
              from funnels_processed.users_pivot),

users_pivot_woo as (select users_pivot.*, type_customer_2.* except(_customer_user), first_purchase_info.* except(_customer_user), latest_purchase_info.* except(_customer_user), 
  transaction_group_user_id.* except(_customer_user), billing_cycles
from users_pivot
left join transaction_group_user_id
on transaction_group_user_id._customer_user= cast(users_pivot.user_id as string)
left join type_customer_2
on type_customer_2._customer_user = cast(users_pivot.user_id as string)
left join first_purchase_info 
on first_purchase_info._customer_user = cast(users_pivot.user_id as string)
left join latest_purchase_info 
on latest_purchase_info._customer_user = cast(users_pivot.user_id as string)
left join billing_cycles_table
on billing_cycles_table._customer_user = cast(users_pivot.user_id as string))

select distinct * from users_pivot_woo