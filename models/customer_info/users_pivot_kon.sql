with customers_info_w_member as ((select 
      customers.customer_id as user_tracking_id,
      customers.email_address as user_email, 
      customers.customer_id as user_id,
      '' as user_registered, 
      customers.first_name, customers.last_name, 
      customers.phone_number as billing_phone,
      customers.city as billing_city,
      customers.state as billing_state, 
      customers.country as billing_country, 
      customers.zipcode as billing_postcode, 
      customers.city as shipping_city, 
      customers.state as shipping_state, 
      customers.country as shipping_country, 
      customers.zipcode as shipping_postcode, 
      '' as wc_last_active, 
      '' as django_user, 
       subscription_date.trial_date_start,
      subscription_date._schedule_trial_end, 
      subscription_date._schedule_cancelled,
      subscription_date._schedule_next_payment,
      -- removing the trial (which counts as 1 in billing_cycles)
      customers.billing_cycles - 1  as billing_cycles,
      customers.current_status as post_status, 
      customers.full_order_id,
      customers.first_product_id as first_order_id, 
      customers.latest_product_id as latest_order_id,
      customers.mktg_affiliate,
      customers.first_aff_id, 
      customers.latest_aff_id, 
      customers.total_purchased as total_amount,
      customers.shipping_cost as order_shipping, 
      customers.sales_tax as order_tax,
      customers.products_purchased,  
      case when customers.billing_cycles is null then 'ecommerce only - konnektive'       when customers.billing_cycles = 1 then 'ecommerce to trial - konnektive' 
      else 'trial to subscription - konnektive'
      end type_customers
from (select distinct * from konnektive.customers ) customers

LEFT JOIN (select customer_id, min(measurement_date) as trial_date_start, date_add(min(measurement_date), INTERVAL 7 day) as _schedule_trial_end, date_add(max(measurement_date), INTERVAL 1 day) as _schedule_cancelled, date_add(date_add(min(measurement_date), INTERVAL 7 day), INTERVAL 1 month) as _schedule_next_payment from konnektive.member_retention_daily group by customer_id) subscription_date
on cast(customers.customer_id as string) = subscription_date.customer_id)),


membership_revenue as (select sales.customer_id, 
  case when refund_member_revenue is null then sales_member_revenue 
  else sales_member_revenue - refund_member_revenue 
  end as member_revenue, 
  0 as ecommerce_revenue,
  case when refund_transaction is null then complete_transaction 
  else complete_transaction - refund_transaction 
  end as orders
FROM (select customer_id, SUM(total_amount) as sales_member_revenue, count(transaction_id) as complete_transaction from (select *
  from konnektive.transactions_cleaned 
  where status != '' and status != '0' and status is not null)
where txn_type = 'SALE' AND response_type = 'SUCCESS'
group by customer_id) sales

LEFT JOIN (select customer_id, SUM(total_amount) as refund_member_revenue, count(transaction_id) as refund_transaction from (select *
  from konnektive.transactions_cleaned 
  where status != '' and status != '0' and status is not null)
where txn_type = 'REFUND' AND response_type = 'SUCCESS'
group by customer_id) refund 
ON sales.customer_id = refund.customer_id),


non_membership_revenue  as (select sales.customer_id,  
  0 as member_revenue, 
  case when refund_ecommerce_revenue is null then sales_ecommerce_revenue
  else sales_ecommerce_revenue - refund_ecommerce_revenue 
  end as ecommerce_revenue, 
  case when refund_transaction is null then complete_transaction 
  else complete_transaction - refund_transaction 
  end as orders
FROM (select customer_id, SUM(total_amount) as sales_ecommerce_revenue, count(transaction_id) as complete_transaction from (select *
  from konnektive.transactions_cleaned 
  where status = '' or status = '0' or status is null)
where txn_type = 'SALE' AND response_type = 'SUCCESS'
group by customer_id) sales

LEFT JOIN (select customer_id, SUM(total_amount) as refund_ecommerce_revenue, count(transaction_id) as refund_transaction from (select *
  from konnektive.transactions_cleaned 
  where status = '' or status = '0' or status is null)
where txn_type = 'REFUND' AND response_type = 'SUCCESS'
group by customer_id) refund 
ON sales.customer_id = refund.customer_id),


revenue as (select customer_id, sum(member_revenue) as member_revenue, 
sum(ecommerce_revenue) as ecommerce_revenue, 
sum(orders) as number_orders
from 
(select customer_id,
        case when member_revenue is null then 0 else member_revenue end member_revenue, 
        case when ecommerce_revenue is null then 0 else ecommerce_revenue end ecommerce_revenue,
        orders
from non_membership_revenue
union all 
select customer_id,
        case when member_revenue is null then 0 else member_revenue end member_revenue, 
        case when ecommerce_revenue is null then 0 else ecommerce_revenue end ecommerce_revenue,
        orders
from membership_revenue)
group by customer_id),


users_pivot_kon as (select * except(type_customers, customer_id, billing_cycles), 
case when type_customers like '%trial to subscription%' and member_revenue = 0 and ecommerce_revenue !=0  then 'ecommerce to trial - konnektive' 
when member_revenue = 0 and ecommerce_revenue = 0 then '' 
else type_customers 
end type_customers, 
case when type_customers like '%trial to subscription%' and member_revenue = 0 then 0
else billing_cycles 
end billing_cycles 
from customers_info_w_member
left join  revenue
on revenue.customer_id = customers_info_w_member.user_id)

select distinct * from users_pivot_kon