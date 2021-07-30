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
  
  case when woo.billing_first_name !='' and woo.billing_first_name is not null then woo.billing_first_name 
  when kon.billing_first_name is not null and (woo.billing_first_name is null or woo.billing_first_name = '') then kon.billing_first_name
  end billing_first_name,

  case when woo.billing_last_name !='' and woo.billing_last_name is not null then woo.billing_last_name 
  when kon.billing_last_name is not null and (woo.billing_last_name is null or woo.billing_last_name = '') then kon.billing_last_name
  end billing_last_name, 

  case when woo.shipping_first_name !='' and woo.shipping_first_name is not null then woo.shipping_first_name 
  when kon.shipping_first_name is not null and (woo.shipping_first_name is null or woo.shipping_first_name = '') then kon.shipping_first_name
  end shipping_first_name,
  
  case when woo.shipping_last_name !='' and woo.shipping_last_name is not null then woo.shipping_last_name 
  when kon.shipping_last_name is not null and (woo.shipping_last_name is null or woo.shipping_last_name = '') then kon.shipping_last_name
  end shipping_last_name, 

  case when woo.billing_address_1 !='' and woo.billing_address_1 is not null then woo.billing_address_1 
  when kon.billing_address_1 is not null and (woo.billing_address_1 is null or woo.billing_address_1 = '') then kon.billing_address_1
  end billing_address_1,

  case when woo.billing_address_2 !='' and woo.billing_address_2 is not null then woo.billing_address_2 
  when kon.billing_address_2 is not null and (woo.billing_address_2 is null or woo.billing_address_2 = '') then kon.billing_address_2
  end billing_address_2,

  case when woo.shipping_company !='' and woo.shipping_company is not null then woo.shipping_company 
  when kon.shipping_company is not null and (woo.shipping_company is null or woo.shipping_company = '') then kon.shipping_company
  end shipping_company,

  case when woo.shipping_address_1 !='' and woo.shipping_address_1 is not null then woo.shipping_address_1 
  when kon.shipping_address_1 is not null and (woo.shipping_address_1 is null or woo.shipping_address_1 = '') then kon.shipping_address_1
  end shipping_address_1,

  case when woo.shipping_address_2 !='' and woo.shipping_address_2 is not null then woo.shipping_address_2 
  when kon.shipping_address_2 is not null and (woo.shipping_address_2 is null or woo.shipping_address_2 = '') then kon.shipping_address_2
  end shipping_address_2,

  case when woo.address1 !='' and woo.address1 is not null then woo.address1 
  when kon.address1 is not null and (woo.address1 is null or woo.address1 = '') then kon.address1
  end address1,

  case when woo.address2 !='' and woo.address2 is not null then woo.address2 
  when kon.address2 is not null and (woo.address2 is null or woo.address2 = '') then kon.address2
  end address2,

  case when woo.shipping_email !='' and woo.shipping_email is not null then woo.shipping_email 
  when kon.shipping_email is not null and (woo.shipping_email is null or woo.shipping_email = '') then kon.shipping_email
  end shipping_email,

  case when woo.shipping_address_id !='' and woo.shipping_address_id is not null then woo.shipping_address_id 
  when kon.shipping_address_id is not null and (woo.shipping_address_id is null or woo.shipping_address_id = '') then kon.shipping_address_id
  end shipping_address_id,

  case when woo.city !='' and woo.city is not null then woo.city 
  when kon.city is not null and (woo.city is null or woo.city = '') then kon.city
  end city,

  case when woo.state !='' and woo.state is not null then woo.state 
  when kon.state is not null and (woo.state is null or woo.state = '') then kon.state
  end state,

  case when woo.country !='' and woo.country is not null then woo.country 
  when kon.country is not null and (woo.country is null or woo.country = '') then kon.country
  end country,

  case when woo.phone !='' and woo.phone is not null then woo.phone 
  when kon.phone is not null and (woo.phone is null or woo.phone = '') then kon.phone
  end phone,


  case when woo.postal_code !='' and woo.postal_code is not null then woo.postal_code 
  when kon.postal_code is not null and (woo.postal_code is null or woo.postal_code = '') then kon.postal_code
  end postal_code,

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
  
  case when woo.billing_postal_code !='' and  woo.billing_postal_code is not null then woo.billing_postal_code 
  when kon.billing_postal_code is not null and (woo.billing_postal_code is null or woo.billing_postal_code='') then kon.billing_postal_code
  end billing_postal_code, 
  
  case when woo.shipping_city !='' and woo.shipping_city is not null then woo.shipping_city 
  when kon.shipping_city is not null and (woo.shipping_city is null or woo.shipping_city='') then kon.shipping_city
  end shipping_city, 
  
  case when woo.shipping_state !='' and woo.shipping_state is not null then woo.shipping_state 
  when kon.shipping_state is not null and (woo.shipping_state is null or woo.shipping_state='') then kon.shipping_state
  end shipping_state, 
  case when woo.shipping_country !='' and woo.shipping_country is not null then woo.shipping_country 
  when kon.shipping_country is not null and (woo.shipping_country is null or woo.shipping_country='') then kon.shipping_country
  end shipping_country, 
  case when woo.shipping_postal_code !='' and woo.shipping_postal_code is not null then woo.shipping_postal_code 
  when kon.shipping_postal_code is not null and (woo.shipping_postal_code is null or woo.shipping_postal_code='') then kon.shipping_postal_code
  end shipping_postal_code, 
  
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
  
  case when kon.customer_profile_id is not null and woo.customer_profile_id is null then cast(kon.customer_profile_id as string)
  when kon.customer_profile_id is null and woo.customer_profile_id is not null then cast(woo.customer_profile_id as string)
  when kon.customer_profile_id is not null and woo.customer_profile_id is not null then concat(kon.customer_profile_id, ', ', woo.customer_profile_id)
  end customer_profile_id,

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

