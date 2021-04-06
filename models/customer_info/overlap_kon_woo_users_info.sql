-- this determines how many woo are in Konnektive and vice versa ~1500. this accumulates customers info based on Woo CRM
with both_kon_woo_id as (select user_id from (select user_id, string_agg(crm_platform) as crm_platform from {{ref('union_kon_n_woo_users_pivot')}} group by user_id) 
      where crm_platform like '%konnektive,woocommerce%'),

non_group_by_element as (select user_tracking_id, user_email, user_id, first_name, last_name, user_registered,
  billing_phone, billing_city, billing_state, 
  billing_country, 
  billing_postcode, 
  shipping_city, shipping_state, shipping_country, shipping_postcode,
  trial_date_start, 
  _schedule_trial_end, _schedule_cancelled, _schedule_next_payment, 
  billing_cycles,
  current_status, type_customers, alpha_testers from {{ref('union_kon_n_woo_users_pivot')}} 
  where user_id in (select user_id from both_kon_woo_id) and crm_platform='woocommerce'),

group_by_element as (select user_id, string_agg(full_order_id) full_order_id, 
  string_agg(cast(first_order_id as string)) as first_order_id, string_agg(cast(latest_order_id as string)) latest_order_id, 
	string_agg(first_mktg_custom_1) first_mktg_custom_1, string_agg(first_mktg_custom_2) first_mktg_custom_2, 
	string_agg(latest_mktg_custom_1) latest_mktg_custom_1, string_agg(latest_mktg_custom_2) latest_mktg_custom_2,
  string_agg(cast(first_aff_id as string)) as first_aff_id, 
  string_agg(cast(latest_aff_id as string)) as latest_aff_id,
  sum(total_amount) total_amount, 
  sum(order_shipping) order_shipping, sum(order_tax) order_tax, 
  string_agg(products_purchased) products_purchased, 
  sum(member_revenue) member_revenue, 
  sum(ecommerce_revenue) ecommerce_revenue,
  sum(number_orders) number_orders, 
  string_agg(crm_platform) crm_platform
 from {{ref('union_kon_n_woo_users_pivot')}} 
 group by user_id),

both_kon_woo_table as(
select kon_woo_id.*, non_group_by_element. * except(user_id), group_by_element.* except(user_id)
from (select user_id from {{ref('union_kon_n_woo_users_pivot')}}  ) kon_woo_id
left join group_by_element
on group_by_element.user_id = kon_woo_id.user_id
left join non_group_by_element
 on non_group_by_element.user_id= kon_woo_id.user_id)

select distinct * from both_kon_woo_table