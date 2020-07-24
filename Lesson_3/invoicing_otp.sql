select
  financial_invoices.financial_invoice_id as invoice_id
  , financial_invoices.issued_ts as invoice_shared_at
  , shipment_timeline.actual_in_full_delivered_at + interval '3 days' as share_due_at
  , shipment_timeline.actual_in_full_delivered_at + interval '3 days' >= financial_invoices.issued_ts as is_invoiced_on_time
  , (datediff(seconds, shipment_timeline.actual_in_full_delivered_at + interval '3 days', financial_invoices.issued_ts)/60./60./24.)::numeric(16,4) as days_to_share
from analytics.entities.financial_invoices
join analytics.entities.shipment_timeline
  on shipment_timeline.shipment_id = financial_invoices.invoiceable_id
  and financial_invoices.invoiceable_type = 'Shipment'
  and shipment_timeline.actual_in_full_delivered_at is not null
join analytics.legacy.supply_shipments on supply_shipments.shipment_id = financial_invoices.invoiceable_id
where
  financial_invoices.financial_invoice_type = 'Invoice'
  and financial_invoices.deleted_ts is null
  and financial_invoices.voided_ts is null
  and financial_invoices.issued_ts is not null
  and financial_invoices.issued_ts>='2020-06-01'
  and days_to_share<=60