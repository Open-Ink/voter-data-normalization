
    
    

with child as (
    select RACE as from_field
    from DBT_VOTER_DATA.cleaned.montana_voters
    where RACE is not null
),

parent as (
    select code as to_field
    from DBT_VOTER_DATA.lookups.races
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


