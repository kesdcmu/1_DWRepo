<<<<<<< HEAD
SELECT -- Week 4 lecture 4 Code Adoptation 
    {{ dbt_utils.star(ref('taxi%2B_zone_lookup')) }}
from {{ ref('taxi%2B_zone_lookup') }}
=======
SELECT
    {{ dbt_utils.star(ref('taxi%2B_zone_lookup')) }}
from {{ ref('taxi%2B_zone_lookup') }}
>>>>>>> 834546b24df8046fd3cfbbd0439eec96d396a7a5
