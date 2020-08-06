select
mu.id,
mu.shipment_id,
s.mode as shipment_mode,
s.load_type as shipment_load_type,
legs.leg_transportation_mode_name as leg_mode_type,
address_type,
update_event_type,
update_date_type,
source,
created_by,
update_lead_hours
from entities.shipment_milestone_updates as mu
left join entities.legs as legs
    on legs.leg_id = mu.leg_id
left join entities.shipment_attributes as s
    on s.shipment_id = mu.shipment_id
where update_created_at >= '2020-07-01'
and update_created_at <= '2020-07-31'
and update_event_type in ('arrival', 'departure')
order by shipment_id desc 
