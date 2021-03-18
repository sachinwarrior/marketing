-- determine what orders are subscription, what orders are upsell associating with that subscription. -> this help determines if the customers take the trial
with subscription_w_transaction as (select subscription_table.post_date_gmt, subscription_table._billing_email, subscription_table._order_total as _order_total,
          parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_start) as trial_date_start, 
          _schedule_trial_end , 
          _schedule_cancelled , 
           case when _schedule_end = '0' and _schedule_next_payment !='0' then  cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_next_payment) as date) 
  when _schedule_end != '0' and _schedule_next_payment = '0' then  cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_end) as date) 
  when _schedule_end != '0' and _schedule_next_payment != '0' then cast(parse_timestamp("%Y-%m-%d %H:%M:%S",_schedule_next_payment) as date) 
  end _schedule_next_payment, 
          subscription_table._customer_user, 
          subscription_table.post_status, 
          number_sub_orders, -- number of orders that associate w subscription (WM Tribe + upsell)
          subscription_table.order_id_sub, 
          subscription_table.post_id as subscription_order_id,  
          transactions.products_purchased 
  from (select *, 
      case when (array_length(split(_subscription_renewal_order_ids_cache, ';')) > 2) then                    substring(split(_subscription_renewal_order_ids_cache,';')[OFFSET(1)], 3,                length(split(_subscription_renewal_order_ids_cache,';')[OFFSET(1)])) end order_id_sub, 
       case when array_length(split(_subscription_renewal_order_ids_cache, ':')) >=3 then 
             split(_subscription_renewal_order_ids_cache,':')[OFFSET(1)] end number_sub_orders
        from funnels_processed.subscription_pivot) subscription_table 
 left join (select * from funnels_processed.transactions where post_date >= '2020-11-01' and post_status not in ('wc-failed', 'auto-draft', 'trash')) transactions 
 on subscription_table.order_id_sub= cast(transactions.id as string) ),

-- Determine the type of customers using a:0, a;1, a:2 in _subscription_ren.._ids_cache and products_purchased 
members_table_1 as (select * from (
                    select *, 
                      case when number_sub_orders ='0' and products_purchased is null and _order_total = '39.95' then 'ecommerce to trial - woocommerce' 
                      when products_purchased like '%Warrior Made Tribe%' then 'trial to subscription - woocommerce' end type_customer 
                   from subscription_w_transaction)
              where type_customer is not null), 

-- getting the lastest information about their member_status or trial_date etc 
members_only_table as  (select members_table_1.* except (products_purchased, post_date_gmt, _billing_email, _order_total, number_sub_orders, order_id_sub)
      from members_table_1 
      inner join (select _customer_user, max(post_date_gmt) as post_date_gmt
                  from members_table_1 group by _customer_user) grouped_members_table_1
      on members_table_1._customer_user = grouped_members_table_1._customer_user and                   members_table_1.post_date_gmt = grouped_members_table_1.post_date_gmt), 



-- grouping all the transactions by user_id, creating ecommerce_revenue, and subscription_revenue
transaction_group_user_id as (select _customer_user as user_id, 
      string_agg(distinct(cast(order_id as string)), ',') as full_order_id, 
      string_agg(mktg_affiliate,',') as mktg_affiliate,
      sum(cast(_order_total as numeric)) as total_amount,
      sum(cast(_order_shipping as numeric)) as order_shipping, 
      sum(cast(_order_tax as numeric)) as order_tax, 
      string_agg(distinct(products_purchased),',') as products_purchased, 
      -- members + ecommerce revenue doesnt include tax
      sum(members_revenue) as members_revenue,
      sum(ecommerce_revenue) as ecommerce_revenue, 
      count(order_id) as number_orders
      from (select *, 
            case when products_purchased like '%Warrior Made Tribe%' then cast(_order_total as               numeric) - cast(_order_tax as numeric) else 0 end members_revenue, 
            case when products_purchased not like '%Warrior Made Tribe%' then cast(_order_total             as numeric) - cast(_order_tax as numeric) else 0 end ecommerce_revenue
            from (select * from funnels_processed.transactions where post_date >= '2020-11-01' and post_status not in ('wc-failed', 'auto-draft', 'trash'))       
            where _customer_user is not null)
      group by _customer_user), 
      
transaction_n_subscription as (select * except(type_customer), 
                case when type_customer is null then 'ecommerce only' else type_customer end                          type_customer 
          from (select members_only_table.* except(_customer_user), transaction_group_user_id.*
                  from transaction_group_user_id left join members_only_table on                                    members_only_table._customer_user = transaction_group_user_id.user_id)
          order by user_id),
-- removing the nested properties in digital_programs column 
users_pivot as (select user_tracking_id, user_email, user_id, user_registered, 
                first_name,last_name, billing_phone,
                shipping_city, shipping_state, shipping_postcode, shipping_country,
                billing_city, billing_state, billing_postcode, billing_country, 
                array_to_string(digital_programs,',') as digital_programs,
                wc_last_active, django_user
                from (SELECT * except(digital_programs), regexp_extract_all(digital_programs, r'"(.*?)"') as digital_programs FROM `wm-gcp-data.funnels_processed.users_pivot`)),

first_purchase_info as (
    SELECT 
      a._customer_user, 
      a.order_id as first_product_id,
      a.mktg_affiliate as first_aff_id
    FROM 
          `funnels_processed.transactions`  A
    INNER JOIN (
                SELECT 
                  _customer_user, 
                  MIN(post_date) as date_created
                FROM
                  `funnels_processed.transactions`
                WHERE post_date >= '2020-11-01' and post_status not in ('wc-failed', 'auto-draft', 'trash')
                GROUP BY 
                  _customer_user 
                ) B
    ON A._customer_user = B._customer_user AND A.post_date = B.date_created
  ), 
  latest_purchase_info as (
    SELECT 
      a._customer_user, 
      a.order_id as latest_product_id,
      a.mktg_affiliate as latest_aff_id
    FROM 
          `funnels_processed.transactions`  A
    INNER JOIN (
                SELECT 
                  _customer_user, 
                  MAX(post_date) as date_created
                FROM
                  `funnels_processed.transactions` 
                WHERE post_date >= '2020-11-01' and post_status not in ('wc-failed', 'auto-draft', 'trash')
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
    (select _customer_user, count(order_id) as complete_sales from      funnels_processed.transactions where products_purchased like '%Warrior Made     Tribe%' and  post_date >= '2020-11-01' and post_status = 'wc-completed' group by _customer_user ) complete_sales_table
left join (select _customer_user, count(order_id) as refund_sales from funnels_processed.transactions where products_purchased like '%Warrior Made Tribe%' and  post_date >= '2020-11-01' and post_status like '%wc-refund%' group by _customer_user ) refund_sales_table
on complete_sales_table._customer_user = refund_sales_table._customer_user)

select users_pivot.*, transaction_n_subscription.* except(user_id), first_purchase_info.* except(_customer_user), latest_purchase_info.* except(_customer_user), billing_cycles
from users_pivot 
left join transaction_n_subscription
on transaction_n_subscription.user_id = cast(users_pivot.user_id as string)
left join first_purchase_info 
on first_purchase_info._customer_user = cast(users_pivot.user_id as string)
left join latest_purchase_info 
on latest_purchase_info._customer_user = cast(users_pivot.user_id as string)
left join billing_cycles_table
on billing_cycles_table._customer_user = cast(users_pivot.user_id as string)