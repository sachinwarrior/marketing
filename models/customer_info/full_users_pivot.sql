
{{ config (
    materialized="table"
)}}

with full_users_pivot as ((select user_tracking_id, 
  user_email,
  user_id, 
  user_registered, 
  first_name,  last_name, 
  billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postcode, 
  shipping_city, shipping_state, shipping_country, shipping_postcode, 
  trial_date_start, 
  cast(_schedule_trial_end as date) as _schedule_trial_end, cast(_schedule_cancelled as date) _schedule_cancelled, cast(_schedule_next_payment as date) as _schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  first_order_id,  latest_order_id, 
	first_mktg_custom_1, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  first_aff_id, latest_aff_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue, 
  number_orders,
  type_customers, 
  alpha_testers, 
	crm_platform
from {{ref('overlap_kon_woo_users_info')}})
UNION ALL 
(select user_tracking_id, 
  user_email,
  user_id, 
  user_registered, 
  first_name,  last_name, 
  billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postcode, 
  shipping_city, shipping_state, shipping_country, shipping_postcode, 
  trial_date_start, 
  _schedule_trial_end,_schedule_cancelled,_schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  cast(first_order_id as string) first_order_id, cast(latest_order_id as string)latest_order_id, 
	first_mktg_custom_1, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  first_aff_id, latest_aff_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue, 
  number_orders,
  type_customers, 
  alpha_testers, 
	crm_platform
from {{ref('union_kon_n_woo_users_pivot')}} 
where user_id not in (select user_id from {{ref('overlap_kon_woo_users_info')}})))

select distinct * from full_users_pivot
