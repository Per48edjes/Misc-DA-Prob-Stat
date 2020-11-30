 select 
    client, 
    client_id,  
    month(quote_accepted_at) as month, 
    year(quote_accepted_at) as year,
    segment_name,
    (sum(case when core_left_wants_customs_service = 'TRUE' then 1 else 0 end)) as count_customs_shipments, 
    (sum(case when wants_flexport_freight = 'TRUE' then 1 else 0 end)) as count_freight_shipments,
    case when (sum(case when wants_flexport_freight = 'TRUE' then 1 else 0 end)) = 0 then NULL
    else (sum(case when core_left_wants_customs_service = 'TRUE' then 1 else 0 end) / sum(case when wants_flexport_freight = 'TRUE' then 1 else 0 end)) end as customs_attach_rate 
   from 
     legacy.bi_customs_shipments
     group by 1, 2, 3, 4, 5
     order by 1, 4, 3

