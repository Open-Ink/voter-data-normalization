





with validation_errors as (

    select
        FILE_DATE, STATE_CODE, COUNTY_CODE, VOTER_ID
    from DBT_VOTER_DATA.cleaned.all_voters
    group by FILE_DATE, STATE_CODE, COUNTY_CODE, VOTER_ID
    having count(*) > 1

)

select *
from validation_errors


