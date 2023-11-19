SELECT -- Week 4 lecture 4 Code Adoptation 
    {{ dbt_utils.star(ref('taxi%2B_zone_lookup')) }}
from {{ ref('taxi%2B_zone_lookup') }}
