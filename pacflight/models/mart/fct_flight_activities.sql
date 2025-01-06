with stg_flights as (
    select *
    from {{ ref("stg_pacflight__flights") }}
),

dim_time as (
    select *
    from {{ ref("dim_time") }}
),

dim_date as (
    select *
    from {{ ref("dim_date") }}
),

dim_airports as (
    select *
    from {{ ref("dim_airports") }}
),

dim_aircrafts as (
    select *
    from {{ ref("dim_aircrafts") }}
),

final_fct_flight_activities as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["sf.flight_id"] ) }} as sk_flight_id,
        sf.flight_id as nk_flight_id,
        sf.flight_no,
        dd1.date_id as scheduled_departure_date_local,
        dd2.date_id as scheduled_departure_date_utc,
        dt1.time_id as scheduled_departure_time_local,
        dt2.time_id as scheduled_departure_time_utc,
        dd3.date_id as scheduled_arrival_date_local,
        dd4.date_id as scheduled_arrival_date_utc,
        dt3.time_id as scheduled_arrival_time_local,
        dt4.time_id as scheduled_arrival_time_utc,
        da1.sk_airport_code as departure_airport,
        da2.sk_airport_code as arrival_airport,
        sf.status,
        dac.sk_aircraft_code as aircraft_code,
        dd5.date_id as actual_departure_date_local,
        dd6.date_id as actual_departure_date_utc,
        dt5.time_id as actual_departure_time_local,
        dt6.time_id as actual_departure_time_utc,
        dd7.date_id as actual_arrival_date_local,
        dd8.date_id as actual_arrival_date_utc,
        dt7.time_id as actual_arrival_time_local,
        dt8.time_id as actual_arrival_time_utc,
        (sf.actual_departure - sf.scheduled_departure) as delay_departure,
        (sf.actual_arrival - sf.scheduled_arrival) as delay_arrival,
        (sf.actual_arrival - sf.actual_departure) as travel_time,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_flights sf
    join dim_date dd1
        on dd1.date_actual = DATE(sf.scheduled_departure)
    join dim_date dd2
        on dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
    join dim_time dt1
        on dt1.time_actual::time = (sf.scheduled_departure)::time
    join dim_time dt2
        on dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
    join dim_date dd3
        on dd3.date_actual = DATE(sf.scheduled_arrival)
    join dim_date dd4
        on dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
    join dim_time dt3 
        on dt3.time_actual::time = (sf.scheduled_arrival)::time
    join dim_time dt4
        on dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
    join dim_airports da1
        on da1.nk_airport_code = sf.departure_airport
    join dim_airports da2
        on da2.nk_airport_code = sf.arrival_airport
    join dim_aircrafts dac 
        on dac.nk_aircraft_code = sf.aircraft_code    
    join dim_date dd5
        on dd5.date_actual = DATE(sf.actual_departure)
    join dim_date dd6
        on dd6.date_actual = DATE(sf.actual_departure AT TIME ZONE 'UTC')
    join dim_time dt5
        on dt5.time_actual::time = (sf.actual_departure)::time
    join dim_time dt6
        on dt6.time_actual::time = (sf.actual_departure AT TIME ZONE 'UTC')::time
    join dim_date dd7
        on dd7.date_actual = DATE(sf.actual_arrival)
    join dim_date dd8
        on dd8.date_actual = DATE(sf.actual_arrival AT TIME ZONE 'UTC')
    join dim_time dt7
        on dt7.time_actual::time = (sf.actual_arrival)::time
    join dim_time dt8
        on dt8.time_actual::time = (sf.actual_arrival AT TIME ZONE 'UTC')::time
)

select * from final_fct_flight_activities