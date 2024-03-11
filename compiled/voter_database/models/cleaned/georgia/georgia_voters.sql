SELECT DISTINCT '2023-09-13'::date                    as FILE_DATE,
                'GA'                                  as STATE_CODE,
                VOTERS.COUNTY                         as COUNTY_CODE,
                VOTERS.VOTER_REGISTRATION_NUMBER      AS VOTER_ID,
                NULL                                  AS PREFIX,
                VOTERS.FIRST_NAME,
                VOTERS.MIDDLE_NAME,
                VOTERS.LAST_NAME,
                VOTERS.SUFFIX                         AS NAME_SUFFIX,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([
                    VOTERS.RESIDENCE_STREET_NUMBER,
                    VOTERS.RESIDENCE_PRE_DIRECTION,
                    VOTERS.RESIDENCE_STREET_NAME,
                    VOTERS.RESIDENCE_STREET_TYPE,
                    VOTERS.RESIDENCE_POST_DIRECTION]) AS RESIDENCE_ADDRESS_LINE_1,
                VOTERS.RESIDENCE_APT_UNIT_NUMBER      AS RESIDENCE_ADDRESS_LINE_2,
                VOTERS.RESIDENCE_CITY                 AS RESIDENCE_ADDRESS_CITY,
                'GA'                                  AS RESIDENCE_ADDRESS_STATE,
                VOTERS.RESIDENCE_ZIPCODE              AS RESIDENCE_ADDRESS_ZIPCODE,
                'USA'                                 AS RESIDENCE_ADDRESS_COUNTRY,
                VOTERS.BIRTH_YEAR,
                NULL                                  AS BIRTH_MONTH,
                NULL                                  AS BIRTH_DATE,
                CASE
                    WHEN STATUS = 'Active' THEN 'A'
                    WHEN STATUS = 'Inactive' THEN 'I'
                    ELSE STATUS
                    END                               as VOTER_STATUS,
                VOTERS.REGISTRATION_DATE,
                VOTERS.LAST_VOTE_DATE                 AS LAST_VOTED_DATE,
                VOTERS.LAST_PARTY_VOTED,
                VOTERS.CONGRESSIONAL_DISTRICT,
                VOTERS.STATE_SENATE_DISTRICT,
                VOTERS.STATE_HOUSE_DISTRICT,
                VOTERS.JUDICIAL_DISTRICT,
                VOTERS.COUNTY_COMMISSION_DISTRICT,
                VOTERS.SCHOOL_BOARD_DISTRICT,
                VOTERS.CITY_COUNCIL_DISTRICT,
                VOTERS.COUNTY_PRECINCT,
                VOTERS.MUNICIPAL_PRECINCT,
                CASE
                    WHEN VOTERS.RACE IN ('Alaskan Native', 'American Indian') THEN 'American Indian or Alaskan Native'
                    ELSE VOTERS.RACE END              AS RACE,
                CASE
                    WHEN VOTERS.GENDER = 'Male' THEN 'M'
                    WHEN VOTERS.GENDER = 'Female' THEN 'F'
                    WHEN VOTERS.GENDER = 'Unknown' THEN 'U'
                    ELSE 'O' END                      AS GENDER,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([
                    VOTERS.MAILING_STREET_NUMBER,
                    VOTERS.MAILING_STREET_NAME])      AS MAILING_ADDRESS_LINE_1,
                MAILING_APT_UNIT_NUMBER               AS MAILING_ADDRESS_LINE_2,
                NULL                                  AS MAILING_LINE_3,
                VOTERS.MAILING_CITY,
                VOTERS.MAILING_STATE,
                VOTERS.MAILING_ZIPCODE,
                VOTERS.MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.GEORGIA_VOTERS_2023_09_13 VOTERS
WHERE NOT (VOTER_REGISTRATION_NUMBER IN ('16561122', '16563765', '16565315') AND MAILING_COUNTRY IS NULL)