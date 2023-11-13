with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

renamed as (

    select
        trim(upper(dispatching_base_num)) as  dispatching_base_num, --some ids are lowercase
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        --sr_flag, always null so chuck it
        --sr_flag was pulled in source and updated, removed from schema  
        trim(upper(affiliated_base_number)) as affiliated_base_number,
        filename

    from source

)

select * from renamed