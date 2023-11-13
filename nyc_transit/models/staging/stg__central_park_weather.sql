with source as (

    select * from {{ source('main', 'central_park_weather') }}

),

renamed as (

    select
        station, --appears to have a relationship with the bases table / created relationship with base_number
        name, --appears to have a relationship with the bases table / created relationship with base_name
        date::date as date, --expected the date to be unique for each location / added unique dbt test
        awnd::double as awnd,
        prcp::double as prcp,
        snow::double as snow,
        snwd::double as snwd,
        tmax::int as tmax,
        tmin::int as tmin,
        filename

    from source

)

select 
    station,
    name, -- added since it appears it was removed as it did not appear in the source.yml
    date, -- added since it appears it was removed as it did not appear in the source.yml
    awnd,
    prcp,
    snow,
    snwd,
    tmax,
    tmin,
    filename
from renamed