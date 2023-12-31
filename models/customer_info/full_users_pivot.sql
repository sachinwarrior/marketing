
{{ config (
    materialized="table"
)}}

with full_users as ((select user_tracking_id, 
  user_email,
  user_id, 
  user_registered, 
  first_name,  last_name, 
  billing_last_name, billing_first_name, 
  shipping_first_name, shipping_last_name,
  billing_address_1, billing_address_2, 
  shipping_address_1, shipping_address_2,
  address1, address2, shipping_company,
  shipping_email, shipping_address_id, 
  city, state, postal_code, country, phone,
  billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postal_code, 
  shipping_city, shipping_state, shipping_country, shipping_postal_code, 
  trial_date_start, 
  cast(_schedule_trial_end as date) as _schedule_trial_end, cast(_schedule_cancelled as date) _schedule_cancelled, cast(_schedule_next_payment as date) as _schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  first_product_id as first_order_id, latest_product_id as latest_order_id, 
	first_mktg_custom_1, first_campaign_id, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  first_aff_id, latest_aff_id,
  customer_profile_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue, 
  number_orders,
  type_customers, 
  'no' as alpha_testers, 
	'woocommerce' as crm_platform
  from  {{ ref('users_pivot_woo') }} )
UNION ALL
  (select cast(user_tracking_id as string) as user_tracking_id, 
  user_email, 
  user_id, 
  cast(NULLIF(user_registered,'') as timestamp) as user_registered,
  first_name, last_name, 
  billing_last_name, billing_first_name, 
  shipping_first_name, shipping_last_name,
  billing_address_1, billing_address_2, 
  shipping_address_1, shipping_address_2,
  address1, address2, shipping_company,
  shipping_email, shipping_address_id, 
  city, state, postal_code, country, phone, 
  billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postal_code, 
  shipping_city, shipping_state, shipping_country, shipping_postal_code,
  trial_date_start, 
  _schedule_trial_end, _schedule_cancelled, _schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  first_order_id, latest_order_id, 
	first_mktg_custom_1, cast(first_campaign_id as string) as first_campaign_id, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  cast(first_aff_id as string) as first_aff_id, 
  cast(latest_aff_id as string) as latest_aff_id,
  cast(customer_profile_id as string) as customer_profile_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue,
  number_orders, 
  type_customers, 
  'no' as alpha_testers, 
	'konnektive' as crm_platform
  from  {{ ref('users_pivot_kon') }} ))

select user_tracking_id, 
  user_email, 
  cast(user_id as string) as user_id, 
  user_registered,
  first_name, last_name, 
  billing_last_name, billing_first_name, 
  shipping_first_name, shipping_last_name,
  billing_address_1, billing_address_2, 
  shipping_address_1, shipping_address_2,
  address1, address2, shipping_company,
  shipping_email, shipping_address_id, 
  city, state, postal_code, country, phone,
  cast(billing_phone as string) as billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postal_code, 
  shipping_city, shipping_state, shipping_country, shipping_postal_code,
  trial_date_start, 
  _schedule_trial_end, _schedule_cancelled, _schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  cast(first_order_id as string) as first_order_id, cast(latest_order_id as string) as latest_order_id, 
	first_mktg_custom_1, first_campaign_id, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  first_aff_id, 
  latest_aff_id,
  customer_profile_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue,
  number_orders, 
  type_customers, 
  alpha_testers, 
  crm_platform from full_users where lower(user_email) not in (select distinct(lower(user_email) ) from {{ref('overlap_kon_woo_users_info')}})
UNION ALL
SELECT user_tracking_id, 
  user_email, 
  cast(user_id as string) as user_id, 
  user_registered,
  first_name, last_name, 
   billing_last_name, billing_first_name, 
  shipping_first_name, shipping_last_name,
  billing_address_1, billing_address_2, 
  shipping_address_1, shipping_address_2,
  address1, address2, shipping_company,
  shipping_email, shipping_address_id, 
  city, state, postal_code, country, phone,
  cast(billing_phone as string) as billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postal_code, 
  shipping_city, shipping_state, shipping_country, shipping_postal_code,
  trial_date_start, 
  _schedule_trial_end, _schedule_cancelled, _schedule_next_payment, 
  billing_cycles,
  current_status, 
  full_order_id, 
  first_order_id, latest_order_id, 
	first_mktg_custom_1, first_campaign_id, first_mktg_custom_2, 
	latest_mktg_custom_1, latest_mktg_custom_2,
  first_aff_id, 
  latest_aff_id,
  customer_profile_id,
  total_amount, 
  order_shipping, order_tax, 
  products_purchased, 
  member_revenue, 
  ecommerce_revenue,
  number_orders, 
  type_customers, 
  'no' as alpha_testers, 
  crm_platform
from {{ref('overlap_kon_woo_users_info')}}
