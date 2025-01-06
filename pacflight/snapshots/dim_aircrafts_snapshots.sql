{% snapshot dim_aircrafts_snapshots %}

{{
    config(
      target_database='pacflight',
      target_schema='public',
      unique_key='sk_aircraft_code',

      strategy='check',
      check_cols=[
            'model',
            'range'
		]
    )
}}

with final_dim_aircrafts as (
	select
		* 
	from {{ ref('dim_aircrafts') }}
)

select * from final_dim_aircrafts

{% endsnapshot %}