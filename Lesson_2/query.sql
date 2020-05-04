select
  s.shipment_id,
  accepted_quote_at,
  tradelane,
  mode
from
  analytics.entities.shipments as s
  join analytics.entities.shipment_attributes as sa
    on s.shipment_id = sa.shipment_id
where
  is_live_shipment = true
  and accepted_quote_at::date > '2018-12-31'
