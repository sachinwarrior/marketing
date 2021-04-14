{% set events_models = [
  ref ('fb_da_1_ads_insights'), 
  ref ('fb_da_3_ads_insights'), 
  ref ('fb_da_4_ads_insights')
] %}
{% for events_model in events_models %}
SELECT
	ad_name,ad_id as fb_ad_id, account_id,
    account_name, campaign_name, adset_name, 
    date_start, date_stop, 
    spend, impressions, ctr, clicks
FROM {{ events_model }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}