
    
    

with child as (
    select VOTER_STATUS as from_field
    from DBT_VOTER_DATA.cleaned.missouri_voters
    where VOTER_STATUS is not null
),

parent as (
    select status_code as to_field
    from DBT_VOTER_DATA.lookups.voter_status
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


