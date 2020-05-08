WITH flxt_quotes_ff AS (with shipment_grain as
      (SELECT
       shipment_id,
       max(case when quote_accepted_date IS NOT NULL THEN quote_freight_estimated_cost END) as won_quote_freight_estimated_cost,
       min(case when is_live_shipment = 'false' THEN quote_freight_estimated_cost END) as lost_quote_freight_estimated_cost

       FROM
       legacy.supply_quotes
       GROUP BY 1),

      air_freight as (
        select
          quote_id,
          case when min(template_id) = 1 then true else false end as is_air_freight_quote
        from legacy.bi_quote_service_items
        group by 1
      ),


    autobook_through_event  as
    (SELECT
      events.created_at as event_time,
      events.user_id,
      events.client_id,
      events.shipment_id,
      events.company_id
      FROM
      core.events
      WHERE events.event_type_id = 7912
    )

      select
      supply_quotes.*
      , CASE WHEN autobook_through_event.shipment_id IS NOT NULL THEN 'Flexport_autobook_quote'
          WHEN supply_quotes.quote_acceptor_is_flexport THEN 'Flexport_impersonate'
          ELSE 'Client' END as quote_acceptor_category
      , shipment_grain.won_quote_freight_estimated_cost
      , shipment_grain.lost_quote_freight_estimated_cost
      , coalesce(shipment_grain.won_quote_freight_estimated_cost,0) + coalesce(shipment_grain.lost_quote_freight_estimated_cost,0) as total_shipment_quote_freight_estimated_cost
      , CASE when supply_quotes.shipment_transportation_mode = 'Ocean' and supply_quotes.lcl = 'LCL' then 'Ocean LCL'
          when supply_quotes.shipment_transportation_mode = 'Ocean' and supply_quotes.lcl = 'Not LCL' then 'Ocean FCL'
          else supply_quotes.shipment_transportation_mode END as shipment_transportation_mode_blended
      , air_freight.is_air_freight_quote
      , MAX(supply_quotes.chargeable_weight) OVER (PARTITION BY supply_quotes.shipment_id) AS quoted_shipment_chargeable_weight
      , MAX(supply_quotes.teu) OVER (PARTITION BY supply_quotes.shipment_id) AS quoted_shipment_teu
      , MAX(supply_quotes.volume) OVER (PARTITION BY supply_quotes.shipment_id) AS quoted_shipment_volume
      , MAX(supply_quotes.transit_time_max) OVER (PARTITION BY supply_quotes.shipment_id) as quoted_transit_time_max
      , MIN(supply_quotes.transit_time_min) OVER (PARTITION BY supply_quotes.shipment_id) as quoted_transit_time_min
      , MIN(case when supply_quotes.shipment_transportation_mode = 'Air'
                then supply_quotes.quote_accepted_date end) OVER (PARTITION BY supply_quotes.client_id)
                as clients_first_air_shipment_acceptance_date
      , max(quote_transit_times.origin_days) OVER (PARTITION BY supply_quotes.shipment_id) as quoted_origin_days
      , max(quote_transit_times.destination_days) OVER (PARTITION BY supply_quotes.shipment_id) as quoted_destination_days
      , max(quote_transit_times.port_to_port_days) OVER (PARTITION BY supply_quotes.shipment_id) as quoted_port_to_port_days
      , min(datediff('hour',supply_quotes.quote_request_created_at,supply_quotes.quote_submitted_at)) OVER (PARTITION BY supply_quotes.shipment_id)  as time_to_share_quote
      , max(supply_quotes.quote_accepted_date) OVER (PARTITION BY supply_quotes.client_id) as clients_last_quote_accepted_date

      from legacy.supply_quotes
      JOIN shipment_grain ON supply_quotes.shipment_id = shipment_grain.shipment_id
      left join core.quotes as quotes on quotes.shipment_id = supply_quotes.shipment_id
        and quotes.first_accepted_at is not null
        and quotes.voided_at is null
      left join core.quote_transit_times as quote_transit_times on quote_transit_times.quote_id = supply_quotes.quote_id
      LEFT JOIN autobook_through_event ON supply_quotes.shipment_id = autobook_through_event.shipment_id
      AND supply_quotes.client_id = autobook_through_event.client_id
      left join air_freight on supply_quotes.quote_id = air_freight.quote_id

       )
  ,  flxt_quoted_transit_times_ocean__edt AS (select
      core_quotes.shipment_id
      , core_quote_transit_times.origin_days
      , core_quote_transit_times.origin_dwell_days
      , core_quote_transit_times.port_to_port_days
      , core_quote_transit_times.destination_dwell_days
      , core_quote_transit_times.destination_days
      , core_quotes.transit_time_min
      , core_quotes.transit_time_max
      , (coalesce(core_quote_transit_times.origin_days,0)
         + coalesce(core_quote_transit_times.origin_dwell_days,0)
         + coalesce(core_quote_transit_times.port_to_port_days,0)
         + coalesce(core_quote_transit_times.destination_dwell_days,0)
         + coalesce(core_quote_transit_times.destination_days,0)) as total_transit_time_days
      , data_source.enum_key as quote_source
      , predicted.origin_days as predicted_origin_days
      , predicted.origin_dwell_days as predicted_origin_dwell_days
      , predicted.port_to_port_days as predicted_port_to_port_days
      , predicted.destination_dwell_days as predicted_destination_dwell_days
      , predicted.destination_days as predicted_destination_days
      , (coalesce(predicted.origin_days,0)
         + coalesce(predicted.origin_dwell_days,0)
         + coalesce(predicted.port_to_port_days,0)
         + coalesce(predicted.destination_dwell_days,0)
         + coalesce(predicted.destination_days,0)) as predicted_total_transit_time_days
      , core_quotes.other_booking_instructions as booking_instructions
    from
      core.quote_transit_times as core_quote_transit_times
      left join core.quotes as core_quotes on core_quote_transit_times.quote_id = core_quotes.id
        and core_quotes.accepted_at is not null
        and core_quotes.voided_at is null

      left join core.data_science_enums as data_source ON core_quote_transit_times.data_source = data_source.enum_index
        and data_source.model_table = 'quote_transit_times'
        and data_source.enum_column = 'data_source'

      left join core.predicted_quote_transit_times as predicted on core_quote_transit_times.quote_id = predicted.quote_id

    where
      shipment_id is not null)
SELECT distinct
	flxt_shipment_details."shipment_id"  AS "flxt_shipment_details.shipment_id",
	TO_CHAR(TO_DATE(
        (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_final_port_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))::date
      end), 'YYYY-MM-DD'))
      ), 'YYYY-MM-DD') AS "flxt_shipments_otp.dynamic_flexport_responsible_completion_date",
	flxt_shipment_details."mode_blended"  AS "flxt_shipment_details.mode_blended",
	CASE
        WHEN (case
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24

        -- Air OTP Calculation
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Door'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Port'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      end
) IS NOT NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_max IS NOT NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_min IS NOT NULL
        AND (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) , 'YYYY-MM-DD HH24:MI:SS'))
      end), 'YYYY-MM-DD')) <= (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_max, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
              then dateadd(day, flxt_quotes_ff.quoted_transit_time_max, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
        AND (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) , 'YYYY-MM-DD HH24:MI:SS'))
      end), 'YYYY-MM-DD')) >= (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
            then dateadd(day, flxt_quotes_ff.quoted_transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
          THEN 'True'
        WHEN (case
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24

        -- Air OTP Calculation
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Door'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Port'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      end
) is NOT NULL
        AND flxt_air_shipments.air_freight_shipment = true
        AND flxt_quotes_ff.quoted_transit_time_min is NOT NULL
        AND flxt_quotes_ff.quoted_transit_time_max is NOT NULL
        AND (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) , 'YYYY-MM-DD HH24:MI:SS'))
      end), 'YYYY-MM-DD')) <= (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_max, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
              then dateadd(day, flxt_quotes_ff.quoted_transit_time_max, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
        AND (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at_local) ), 'YYYY-MM-DD'))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD'))
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at_local) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) , 'YYYY-MM-DD HH24:MI:SS'))
      end), 'YYYY-MM-DD')) >= (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
            then dateadd(day, flxt_quotes_ff.quoted_transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
          then 'True'
        WHEN (case
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24

        -- Air OTP Calculation
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Door'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Port'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      end
) IS NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_max IS NOT NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_min IS NOT NULL
        AND current_date()< (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
            then dateadd(day, flxt_quotes_ff.quoted_transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
          THEN 'Not Eligible'
        WHEN (case
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
      and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24

        -- Air OTP Calculation
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Door'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Port to Port'
        then datediff(hours, coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Port'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))) / 24
      when flxt_air_shipments.air_freight_shipment = true
      and flxt_quotes_ff.freight_type = 'Door to Door'
        then datediff(hours, (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS'))) / 24
      end
) IS NOT NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_max IS NOT NULL
        AND flxt_quoted_transit_times_ocean__edt.transit_time_min IS NOT NULL
        AND current_date() < (TO_CHAR(TO_DATE(case
          when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl', 'flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
              then dateadd(day, flxt_quoted_transit_times_ocean__edt.transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          when flxt_air_shipments.air_freight_shipment = true
            then dateadd(day, flxt_quotes_ff.quoted_transit_time_min, (TO_CHAR(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Door', 'Port to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_departure_port_arrived_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Port', 'Door to Door')
          then (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) , 'YYYY-MM-DD HH24:MI:SS'))::date
      end, 'YYYY-MM-DD HH24:MI:SS')))::date
          else null
        end), 'YYYY-MM-DD'))
          THEN 'Not Eligible'
        ELSE 'False'
      END AS "flxt_shipments_otp.flexport_responsible_transit_on_time",
	CASE WHEN CASE
      when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl' then
        case
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door' then
        case
          WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            THEN TRUE
        else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port' then
          case
            when (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door' then
          case
            WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port' then
        case
            WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
            AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        end
      when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment') then
        case
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door' then
        case
          when (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_delivered_at_local) ), 'YYYY-MM-DD'))) IS NOT NULL
            THEN TRUE
        else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port' then
          case
          when (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door' then
          case
          WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_delivered_at_local) ), 'YYYY-MM-DD'))) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port' then
        case
          WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        end
      when flxt_air_shipments.air_freight_shipment = true then
      case
        when flxt_quotes_ff.freight_type = 'Port to Door' then
          case
          when (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_delivered_at_local) ), 'YYYY-MM-DD'))) IS NOT NULL
          then TRUE
        else false end
        when flxt_quotes_ff.freight_type = 'Port to Port' then
          case
          when (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_quotes_ff.freight_type = 'Door to Door' then
          case
          WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at_local) ), 'YYYY-MM-DD')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_delivered_at_local) ), 'YYYY-MM-DD'))) IS NOT NULL
              THEN TRUE
          else FALSE end
        when flxt_quotes_ff.freight_type = 'Door to Port' then
        case
          WHEN (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_cargo_ready_date_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_origin_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_consolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_first_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_arrived_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
          AND (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at_local) ), 'YYYY-MM-DD')) IS NOT NULL
              THEN TRUE
          else FALSE end
      end
    end THEN 'Yes' ELSE 'No' END
 AS "flxt_shipments_otp.all_transit_milestones_complete"
FROM legacy.prep_clients  AS flxt_prep_clients_shipments
LEFT JOIN legacy.prep_shipment_details  AS flxt_shipment_details ON (flxt_shipment_details."client_id") = flxt_prep_clients_shipments.client_id
          AND (flxt_shipment_details."is_live_shipment") = true -- this condition ensure cleans data

LEFT JOIN legacy.prep_shipment_container_movements  AS flxt_shipment_container_movements ON (flxt_shipment_details."shipment_id") = flxt_shipment_container_movements.shipment_id
LEFT JOIN legacy.bi_address_locations  AS destination ON flxt_shipment_container_movements.destination_address_id = (destination."address_id")
LEFT JOIN legacy.bi_air_shipments  AS flxt_air_shipments ON flxt_shipment_container_movements.shipment_id = flxt_air_shipments.shipment_id
LEFT JOIN legacy.prep_shipment_movements  AS flxt_shipments_otp ON (flxt_shipment_details."shipment_id") = flxt_shipments_otp.shipment_id
LEFT JOIN flxt_quotes_ff ON (flxt_shipment_details."shipment_id") = flxt_quotes_ff.shipment_id
LEFT JOIN legacy.supply_ocean_shipments  AS flxt_supply_ocean_shipments__final ON (flxt_shipment_details."shipment_id") = flxt_supply_ocean_shipments__final.shipment_id
LEFT JOIN flxt_quoted_transit_times_ocean__edt ON (flxt_shipment_details."shipment_id") = flxt_quoted_transit_times_ocean__edt.shipment_id

WHERE ((((flxt_shipment_details."freight_type") IS NOT NULL))) AND ((UPPER(flxt_shipment_details."transportation_status" ) = UPPER('Final Destination'))) AND (flxt_shipment_details."wants_flexport_freight") AND ((UPPER(destination."region" ) = UPPER('Europe'))) AND ((((case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_final_port_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))::date
      end) >= ((DATEADD('month', -3, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE)))) AND (case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_final_port_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))::date
      end) < ((DATEADD('month', 6, CAST(DATE_TRUNC('quarter', DATEADD('month', -3, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE))) AS DATE)))))))
GROUP BY 1,TO_DATE(
        (TO_CHAR(TO_DATE(case
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type = 'fcl'
        and flxt_supply_ocean_shipments__final.freight_type_name in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_final_port_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_final_port_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Port to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Port'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) ), 'YYYY-MM-DD'))::date)
        when flxt_supply_ocean_shipments__final.ocean_vol_type in ('fcl','flxt lcl consol child shipment','oceanmatch headload shipment','coloaded lcl shipment')
        and flxt_supply_ocean_shipments__final.freight_type_name = 'Door to Door'
          then coalesce((TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) ), 'YYYY-MM-DD'))::date, (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD'))::date)
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Port to Port', 'Door to Port')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_ready_for_pickup_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_deconsolidation_departed_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_arrival_port_departed_at) , 'YYYY-MM-DD HH24:MI:SS')))::date
        when flxt_air_shipments.air_freight_shipment = true
        and flxt_quotes_ff.freight_type in ('Door to Door', 'Port to Door')
          then coalesce((TO_CHAR(TO_TIMESTAMP_NTZ(flxt_shipments_otp.actual_in_full_delivered_at) , 'YYYY-MM-DD HH24:MI:SS')), (TO_CHAR(TO_DATE(TO_TIMESTAMP_NTZ(flxt_shipments_otp.scheduled_in_full_delivered_at) ), 'YYYY-MM-DD')))::date
      end), 'YYYY-MM-DD'))
      ),3,4,5
ORDER BY 3
