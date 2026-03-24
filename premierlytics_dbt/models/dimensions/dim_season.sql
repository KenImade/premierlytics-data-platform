with source as (
    select * from {{ref('int_season_spine')}}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['season_id']) }} as season_sk,
        season_id,
        season_label,
        is_current
    from source
)

select * from final