select
  financial_invoices.financial_invoice_id as invoice_id,
  financial_invoices.issuing_entity_legal_name as issuing_entity,
  financial_invoices.base_currency_code,
  financial_invoices.total_net_revenue_usd_amount as total_revenue_usd
from analytics.entities.financial_invoices
where
  financial_invoices.invoiceable_type = 'Shipment'
  and financial_invoices.financial_invoice_type = 'Invoice'
  and financial_invoices.deleted_ts is null
  and financial_invoices.voided_ts is null
  and financial_invoices.issued_ts is not null
  and financial_invoices.issued_ts >= '2020-01-01'
  and financial_invoices.issued_ts < '2020-07-01'