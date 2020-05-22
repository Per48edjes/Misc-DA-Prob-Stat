/*
In Transit Collateral and Outstanding Balance for Python Data Viz course
*/

with
  capital_shipments as (
    select distinct invoiceable_id as shipment_id
    from
      analytics.entities.financial_invoices
    where
        invoiceable_type = 'Shipment'
    and financial_invoice_type = 'Invoice'
    and (deleted_ts is null and voided_ts is null)
    and capital_total_usd_amount > 0
  ),

  ci_shipments as (
    select shipment_id, ci_value
    from
      analytics.legacy.bi_customs_shipments
    where ci_value is not null
  ),

  med_ci_cte as (
    select
      client_id,
      sum(ci_value)      as total_ci_value,
      count(shipment_id) as num_shipments,
      median(ci_value)   as median_ci_value
    from
      analytics.legacy.bi_customs_shipments
    where
    -- Don't edit this
        ci_keyed_date between '2019-01-01' and '2019-12-31'
    and ci_value is not null
    group by
      1
  ),

  shipments_cte as (
    select
      sa.shipment_id,
      sa.freight_type,
      case
        when freight_type = 'Door to Door'
          then st.actual_origin_departed_at
        when freight_type = 'Door to Port'
          then st.actual_origin_departed_at
        when freight_type = 'Port to Door'
          then st.actual_departure_port_departed_at
        when freight_type = 'Port to Port'
          then st.actual_departure_port_departed_at
      end                                                       as in_transit_start_ts,
      coalesce(
        case
          when freight_type = 'Door to Door'
            then st.actual_in_full_delivered_at
          when freight_type = 'Door to Port'
            then st.actual_arrival_port_arrived_at
          when freight_type = 'Port to Door'
            then st.actual_in_full_delivered_at
          when freight_type = 'Port to Port'
            then st.actual_arrival_port_arrived_at
        end,
        st.actual_first_delivered_at,
        st.actual_arrival_port_arrived_at)                      as in_transit_end_ts,
      ci_shipments.ci_value                                     as actual_ci_usd_amount,
      med_ci_cte.median_ci_value                                as median_2019_ci_usd_amount,
      coalesce(actual_ci_usd_amount, median_2019_ci_usd_amount) as imputed_ci_usd_amount
    from
      analytics.entities.shipment_attributes sa
      left join analytics.entities.shipment_timeline st
        on sa.shipment_id = st.shipment_id
      left join analytics.legacy.prep_shipments ps
        on sa.shipment_id = ps.shipment_id
      join capital_shipments
        on sa.shipment_id = capital_shipments.shipment_id
      left join analytics.entities.shipment_involved_parties parties
        on sa.shipment_id = parties.shipment_id
      left join ci_shipments
        on sa.shipment_id = ci_shipments.shipment_id
      left join med_ci_cte
        on parties.client_id = med_ci_cte.client_id
    where
      sa.is_live_shipment = true
  ),

  dates_cte as (
    select *
    from
      analytics.legacy.dates
    where date_day between '2019-01-01' and '2019-12-31'
  ),

  prep_in_transit as (
    select
      date_day as calendar_date,
      round(
        sum(
          case
            when calendar_date between in_transit_start_ts and in_transit_end_ts
              then imputed_ci_usd_amount
            else 0
          end
          ),
        2
        )      as in_transit_ci_value
    from
      dates_cte
      cross join shipments_cte
    group by
      1
    order by
      1
  ),

  prep_outstanding as (
    select
      date_day as calendar_date,
      sum(
        case
          when repay.funds_outlaid_utc_dt <= calendar_date
            and (
              (
                repay.is_collected_payment = true and repay.actual_repayment_utc_dt > calendar_date)
                or
                (
                  repay.is_charged_off = true and repay.charged_off_utc_dt > calendar_date)
                or
                (
                  repay.is_collected_payment = false and repay.is_charged_off = false)
              )
            then repay.principal_repayment_usd_amount
          else 0
        end
        )      as outstanding_principal
    from
      dates_cte
      cross join analytics.entities.capital_repayments repay
      left join analytics.entities.capital_originations origin
      on repay.capital_origination_sk = origin.capital_origination_sk
    where
        repay.is_deleted = false
    and origin.is_live_origination = true
    group by
      1
    order by
      1
  )

select
  dates_cte.date_day as calendar_date,
  in_transit_ci_value,
  outstanding_principal
from
  dates_cte
  left join prep_in_transit
    on dates_cte.date_day = prep_in_transit.calendar_date
  left join prep_outstanding
    on dates_cte.date_day = prep_outstanding.calendar_date
order by
  1
;
