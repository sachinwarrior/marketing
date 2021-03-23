{{ config (
    materialized="table"
)}}
SELECT 
        *
        except(_ingest_datetime)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY partition_value ORDER BY _ingest_datetime DESC) as row_num
        FROM (

    SELECT 
        CONCAT(
            date,
            affiliate_id,
            COALESCE(campaign_name,''),
            COALESCE(CAST(goal_id as STRING),''),
            COALESCE(CAST(offer_id as STRING),''),
            COALESCE(affiliate_info1,'')
        ) as partition_value,
        substr(cast(_ingest_datetime as string),1,19) _ingest_datetime,
        affiliate_id,
        source_id,
        affiliate_name,
        goal,
        date,
        goal_id,
        affiliate_info1,
        offer_id,	
        campaign_name,
        clicks,
        conversions,
        payout
    FROM
        hasoffers.spend
    UNION ALL (
    SELECT
        CONCAT(
            date,
            affiliate_id,
            COALESCE(campaign_name,''),
            COALESCE(CAST(goal_id as STRING),''),
            COALESCE(CAST(offer_id as STRING),''),
            COALESCE(affiliate_info1,'')
        ) as partition_value,
        substr(cast(_ingest_datetime as string),1,19) _ingest_datetime,
        affiliate_id,	
        source_id,
        affiliate_name,
        goal,
        CAST(date as DATE) as date,
        goal_id,
        affiliate_info1,
        offer_id,
        campaign_name,
        clicks,
        conversions,
        payout
    FROM
        hasoffers.spend_dump
        )
      )

    )
      WHERE 
   row_num = 1