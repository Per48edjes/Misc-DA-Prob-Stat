select
  client_id,
  segment,
  estimated_lifetime_weeks,
  estimated_churn_date,
  has_died
from analytics.legacy.prep_client_lifetimes
where
  segment = 'SMB'
  and date_part(year, first_shipment_date) = 2015
