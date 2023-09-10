with

movements as (

    select * from {{ ref('fct_movements') }}

) 

select 
    train_id
    , count(*) as record_count
from movements

where variation_status = 'ON TIME'
group by train_id
order by 2


