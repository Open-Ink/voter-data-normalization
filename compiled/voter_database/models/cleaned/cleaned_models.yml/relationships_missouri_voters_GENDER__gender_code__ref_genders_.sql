
    
    

with child as (
    select GENDER as from_field
    from DBT_VOTER_DATA.cleaned.missouri_voters
    where GENDER is not null
),

parent as (
    select gender_code as to_field
    from DBT_VOTER_DATA.lookups.genders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


