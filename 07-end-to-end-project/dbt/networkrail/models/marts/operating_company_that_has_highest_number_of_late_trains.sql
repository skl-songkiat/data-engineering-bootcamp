with

movements as (

    select * from {{ ref('fct_movements') }}

) 

select 
    train_id
    , count(*) as record_count
from movements

where variation_status = 'LATE'
group by train_id
order by 2


