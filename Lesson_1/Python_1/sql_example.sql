with tpeb_plus_quotes as (
    select
      to_char(DATEADD(day, -1*(DATE_PART(WEEKDAY, accepted_at)-1), accepted_at), 'YYYY-MM-DD') as quote_accepted_week,
      offerings.service_level_resolved as count_premium_shipments
    from
      legacy.bi_quote_service_items
      left join core.ocean_lcl_offerings as offerings
      on offerings.id = bi_quote_service_items.original_service_offering_id
    where
    slug = 'lcl_ocean_freight'
    and accepted_at is not null
    and offerings.service_level_resolved in ('plus', 'premium')
    qualify row_number() over (partition by shipment_id order by shipment_id desc) = 1
   order by quote_accepted_week desc
  )
select
  quote_accepted_week,
  count_premium_shipments as shipment_type,
  count(count_premium_shipments) as count_of_shipments_booked
from tpeb_plus_quotes
group by 1, 2
order by quote_accepted_week asc;

