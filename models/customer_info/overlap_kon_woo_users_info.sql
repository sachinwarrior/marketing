with woo as (select distinct * from (select * except(user_email, total_amount, order_shipping,order_tax, member_revenue, number_orders, ecommerce_revenue), 
  lower(user_email) as woo_email, 
  case when total_amount is null then 0 
  else total_amount end total_amount, 
  case when order_shipping is null then 0 
  else order_shipping end order_shipping,
  case when order_tax is null then 0 
  else order_tax end order_tax, 
  case when member_revenue is null then 0 
  else member_revenue end member_revenue,
  case when ecommerce_revenue is null then 0 
  else ecommerce_revenue end ecommerce_revenue, 
  case when number_orders is null then 0 
  else number_orders end number_orders, 
  'woocommerce' as crm_platform from {{ref('users_pivot_woo')}})),
kon as (select distinct * from (select * except(user_email, total_amount, order_shipping,order_tax, member_revenue, number_orders, ecommerce_revenue), 
  lower(user_email) as kon_email, 
  case when total_amount is null then 0 
  else total_amount end total_amount, 
  case when order_shipping is null then 0 
  else order_shipping end order_shipping,
  case when order_tax is null then 0 
  else order_tax end order_tax, 
  case when member_revenue is null then 0 
  else member_revenue end member_revenue,
  case when ecommerce_revenue is null then 0 
  else ecommerce_revenue end ecommerce_revenue, 
  case when number_orders is null then 0 
  else number_orders end number_orders, 
  'konnektive' as crm_platform from {{ref('users_pivot_kon')}}))


select distinct * from (select 
  case when woo.user_tracking_id !='' and woo.user_tracking_id is not null then woo.user_tracking_id 
  else cast(kon.user_tracking_id as string)
  end user_tracking_id,
  
  kon_email as user_email, 
  concat(kon.user_id,', ', woo.user_id) as user_id, 
  
  case when woo.user_registered is not null then woo.user_registered 
  when kon.user_registered is not null and (woo.user_registered is null) then cast(NULLIF(kon.user_registered,'') as timestamp)
  end user_registered,
  
  case when woo.first_name !='' and woo.first_name is not null then woo.first_name 
  when kon.first_name is not null and (woo.first_name is null or woo.first_name = '') then kon.first_name
  end first_name,
  case when woo.last_name !='' and woo.last_name is not null then woo.last_name 
  when kon.last_name is not null and (woo.last_name is null or woo.last_name = '') then kon.last_name
  end last_name, 
  
  case when woo.billing_phone !='' and  woo.billing_phone is not null then woo.billing_phone 
  when kon.billing_phone is not null and (woo.billing_phone is null or woo.billing_phone='') then kon.billing_phone
  end billing_phone,
  
  case when woo.billing_city !='' and woo.billing_city is not null then woo.billing_city 
  when kon.billing_city is not null and (woo.billing_city is null or woo.billing_city='') then kon.billing_city
  end billing_city, 
  
  case when woo.billing_state !='' and woo.billing_state is not null then woo.billing_state 
  when kon.billing_state is not null and (woo.billing_state is null or woo.billing_state='') then kon.billing_state
  end billing_state, 
  
  case when woo.billing_country !='' and woo.billing_country is not null then woo.billing_country 
  when kon.billing_country is not null and (woo.billing_country is null or woo.billing_country='') then kon.billing_country
  end billing_country, 
  
  case when woo.billing_postcode !='' and  woo.billing_postcode is not null then woo.billing_postcode 
  when kon.billing_postcode is not null and (woo.billing_postcode is null or woo.billing_postcode='') then kon.billing_postcode
  end billing_postcode, 
  
  case when woo.shipping_city !='' and woo.shipping_city is not null then woo.shipping_city 
  when kon.shipping_city is not null and (woo.shipping_city is null or woo.shipping_city='') then kon.shipping_city
  end shipping_city, 
  
  case when woo.shipping_state !='' and woo.shipping_state is not null then woo.shipping_state 
  when kon.shipping_state is not null and (woo.shipping_state is null or woo.shipping_state='') then kon.shipping_state
  end shipping_state, 
  case when woo.shipping_country !='' and woo.shipping_country is not null then woo.shipping_country 
  when kon.shipping_country is not null and (woo.shipping_country is null or woo.shipping_country='') then kon.shipping_country
  end shipping_country, 
  case when woo.shipping_postcode !='' and woo.shipping_postcode is not null then woo.shipping_postcode 
  when kon.shipping_postcode is not null and (woo.shipping_postcode is null or woo.shipping_postcode='') then kon.shipping_postcode
  end shipping_postcode, 
  
  case when woo.trial_date_start is not null then woo.trial_date_start 
  when kon.trial_date_start is not null and (woo.trial_date_start is null) then kon.trial_date_start
  end trial_date_start, 
  case when woo._schedule_trial_end  is not null then cast(woo._schedule_trial_end as date)
  when kon._schedule_trial_end is not null and (woo._schedule_trial_end is null) then kon._schedule_trial_end
  end _schedule_trial_end, 
  case when woo._schedule_cancelled  is not null then cast(woo._schedule_cancelled as date)
  when kon._schedule_cancelled is not null and (woo._schedule_cancelled is null) then kon._schedule_cancelled
  end _schedule_cancelled, 
  
  case when woo._schedule_next_payment  is not null then cast(woo._schedule_next_payment as date)
  when kon._schedule_next_payment is not null and (woo._schedule_next_payment is null) then kon._schedule_next_payment
  end _schedule_next_payment, 
  
  case when woo.billing_cycles is not null then woo.billing_cycles
  when kon.billing_cycles is not null and (woo.billing_cycles is null) then kon.billing_cycles
  end billing_cycles, 
  
  case when woo.current_status like '%active%' then woo.current_status
  when kon.current_status like '%active%' then kon.current_status 
  when woo.current_status is not null then woo.current_status 
  when kon.current_status is not null and (woo.current_status is null) then kon.current_status
  end current_status, 
  case when kon.full_order_id is not null and woo.full_order_id is null then kon.full_order_id
  when kon.full_order_id is null and woo.full_order_id is not null then woo.full_order_id
  when kon.full_order_id is not null and woo.full_order_id is not null then concat(kon.full_order_id, ', ', woo.full_order_id) 
  end full_order_id, 
  
  case when kon.first_order_id is not null and woo.first_product_id is null then cast(kon.first_order_id as string)
  when kon.first_order_id is null and woo.first_product_id is not null then cast(woo.first_product_id as string)
  when kon.first_order_id is not null and woo.first_product_id is not null then concat(kon.first_order_id, ', ', woo.first_product_id)
  end first_order_id,
  
  case when kon.latest_order_id is not null and woo.latest_product_id is null then cast(kon.latest_order_id as string)
  when kon.latest_order_id is null and woo.latest_product_id is not null then cast(woo.latest_product_id as string)
  when kon.latest_order_id is not null and woo.latest_product_id is not null then concat(kon.latest_order_id, ', ', woo.latest_product_id)
  end latest_order_id,
  case when kon.first_mktg_custom_1 is not null and woo.first_mktg_custom_1 is null then cast(kon.first_mktg_custom_1 as string)
  when kon.first_mktg_custom_1 is null and woo.first_mktg_custom_1 is not null then cast(woo.first_mktg_custom_1 as string)
  when kon.first_mktg_custom_1 is not null and woo.first_mktg_custom_1 is not null then concat(kon.first_mktg_custom_1, ', ', woo.first_mktg_custom_1)
  end first_mktg_custom_1,
  
  case when kon.first_mktg_custom_2 is not null and woo.first_mktg_custom_2 is null then cast(kon.first_mktg_custom_2 as string)
  when kon.first_mktg_custom_2 is null and woo.first_mktg_custom_2 is not null then cast(woo.first_mktg_custom_2 as string)
  when kon.first_mktg_custom_2 is not null and woo.first_mktg_custom_2 is not null then concat(kon.first_mktg_custom_2, ', ', woo.first_mktg_custom_2)
  end first_mktg_custom_2,
  
  case when kon.latest_mktg_custom_1 is not null and woo.latest_mktg_custom_1 is null then cast(kon.latest_mktg_custom_1 as string)
  when kon.latest_mktg_custom_1 is null and woo.latest_mktg_custom_1 is not null then cast(woo.latest_mktg_custom_1 as string)
  when kon.latest_mktg_custom_1 is not null and woo.latest_mktg_custom_1 is not null then concat(kon.latest_mktg_custom_1, ', ', woo.latest_mktg_custom_1)
  end latest_mktg_custom_1,
  
   case when kon.latest_mktg_custom_2 is not null and woo.latest_mktg_custom_2 is null then cast(kon.latest_mktg_custom_2 as string)
  when kon.latest_mktg_custom_2 is null and woo.latest_mktg_custom_2 is not null then cast(woo.latest_mktg_custom_2 as string)
  when kon.latest_mktg_custom_2 is not null and woo.latest_mktg_custom_2 is not null then concat(kon.latest_mktg_custom_2, ', ', woo.latest_mktg_custom_2)
  end latest_mktg_custom_2,
  
  case when kon.first_aff_id is not null and woo.first_aff_id is null then cast(kon.first_aff_id as string)
  when kon.first_aff_id is null and woo.first_aff_id is not null then cast(woo.first_aff_id as string)
  when kon.first_aff_id is not null and woo.first_aff_id is not null then concat(kon.first_aff_id, ', ', woo.first_aff_id)
  end first_aff_id,
  
  case when kon.latest_aff_id is not null and woo.latest_aff_id is null then cast(kon.latest_aff_id as string)
  when kon.latest_aff_id is null and woo.latest_aff_id is not null then cast(woo.latest_aff_id as string)
  when kon.latest_aff_id is not null and woo.latest_aff_id is not null then concat(kon.latest_aff_id, ', ', woo.latest_aff_id)
  end latest_aff_id,
  
  kon.total_amount + woo.total_amount as total_amount, 
  kon.order_shipping + woo.order_shipping as order_shipping, 
  kon.order_tax + woo.order_tax as order_tax, 
  case when kon.products_purchased is not null and woo.products_purchased is null then cast(kon.products_purchased as string)
  when kon.products_purchased is null and woo.products_purchased is not null then cast(woo.products_purchased as string)
  when kon.products_purchased is not null and woo.products_purchased is not null then concat(kon.products_purchased, ', ', woo.products_purchased)
  end products_purchased,
  kon.member_revenue+woo.member_revenue as member_revenue, 
  kon.ecommerce_revenue + woo.ecommerce_revenue as ecommerce_revenue, 
  kon.number_orders+woo.number_orders as number_orders, 
  case when kon.type_customers is not null and woo.type_customers is not null and kon.current_status is not null and woo.current_status is null then kon.type_customers
  when kon.type_customers is not null and woo.type_customers is not null and woo.current_status is not null and kon.current_status is null then woo.type_customers
  when kon.type_customers is null and kon.type_customers ='' and woo.type_customers is not null then woo.type_customers
  when woo.type_customers is null and (kon.type_customers is not null and kon.type_customers!='') then kon.type_customers
  when woo.type_customers = kon.type_customers then woo.type_customers
  when woo.type_customers like '%trial%' then woo.type_customers
  when kon.type_customers like '%trial%' then kon.type_customers
  end type_customers,
  
  concat(kon.crm_platform, ', ',woo.crm_platform) as crm_platform,
  
from woo
inner join kon
on (kon_email) = (woo_email) )

