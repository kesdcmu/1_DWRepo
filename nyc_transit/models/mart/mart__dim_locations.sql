SELECT
    {{ dbt_utils.star(ref('taxi%2B_zone_lookup')) }}
from {{ ref('taxi%2B_zone_lookup') }}