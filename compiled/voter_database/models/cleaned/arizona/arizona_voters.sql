SELECT '2023-11-05'::date                       as FILE_DATE,
       'AZ'                                     as STATE_CODE,
       REGISTRANT_COUNTY                        as COUNTY_CODE,
       COALESCE(VOTER_ID, '') || '|'
           || COALESCE(VRAZ_VOTER_ID, '') || '|'
           || GEN_VOTER_ID || '|'               AS VOTER_ID,
       NULL                                     AS PREFIX,
       FIRST_NAME                               AS FIRST_NAME,
       MIDDLE_NAME                              AS MIDDLE_NAME,
       LAST_NAME                                AS LAST_NAME,
       SUFFIX                                   AS NAME_SUFFIX,
       ADDRESS_LINE1                            AS RESIDENCE_ADDRESS_LINE_1,
       ADDRESS_LINE2                            AS RESIDENCE_ADDRESS_LINE_2,
       RESIDENCE_CITY                           AS RESIDENCE_ADDRESS_CITY,
       COALESCE(RESIDENCE_STATE, 'AZ')          AS RESIDENCE_ADDRESS_STATE,
       RESIDENCE_ZIP                            AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                    AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR_OF_BIRTH                            AS BIRTH_YEAR,
       NULL                                     AS BIRTH_MONTH,
       NULL                                     AS BIRTH_DATE,
       CASE
           WHEN STATUS = 'Active' THEN 'A'
           WHEN STATUS = 'Inactive' THEN 'I'
           WHEN STATUS = 'Suspense' THEN 'S'
           WHEN STATUS = 'R' THEN 'I'
           WHEN STATUS IS NULL THEN 'A'
           ELSE STATUS
           END                                  as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY') AS REGISTRATION_DATE,
       NULL                                     AS LAST_VOTED_DATE,
       PARTY                                    AS LAST_PARTY_VOTED,
       CONGRESSIONAL                            AS CONGRESSIONAL_DISTRICT,
       NULL                                     AS STATE_SENATE_DISTRICT,
       LEGISLATIVE                              AS STATE_HOUSE_DISTRICT,
       NULL                                     AS JUDICIAL_DISTRICT,
       NULL                                     AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL                                   AS SCHOOL_BOARD_DISTRICT,
       NULL                                     AS CITY_COUNCIL_DISTRICT,
       PRECINCT                                 AS COUNTY_PRECINCT,
       NULL                                     AS MUNICIPAL_PRECINCT,
       NULL                                     AS RACE,
       NULL                                     AS GENDER,
       MAILING_ADDRESS                          AS MAILING_ADDRESS_LINE_1,
       MAILING_ADDRESS2                         AS MAILING_ADDRESS_LINE_2,
       NULL                                     AS MAILING_LINE_3,
       MAILING_CITY                             AS MAILING_CITY,
       MAILING_STATE                            AS MAILING_STATE,
       MAILING_ZIP                              AS MAILING_ZIPCODE,
       MAILING_COUNTRY                          AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.ARIZONA_VOTERS_2023_11_05
WHERE GEN_VOTER_ID NOT IN (
                           'UkFZTU9OREVBUkxIQU1NT04xODYwRTMwMDBTT1VUSDg2MDIxTU9IQVZF',
                           'RlJFREVSSUNLU1RFVkVOU0FOQ0hFWjI4MzVTUk9CSU5TT05BVkU4NTU1Mi01MjY5R1JBSEFN',
                           'U1VTQU5NQVJZQU5ERVJTT04xNzgyTU9OVEFOQVZJU1RBODY0MDNNT0hBVkU=',
                           'UkFRVUVMQU5HRUxJTkFNQVpBMjMwOVMxMlRIQVZFODU1NDZHUkFIQU0=',
                           'SkVTU0lDQU1BUklFRUlEU09OMzQyMVNIRUlLRFI4NjQwNE1PSEFWRQ==',
                           'Uk9EUklHT0xBUkFIRVJNT1NJTExPNTUwRVJBSU5CT1dSRDg2NDMyTU9IQVZF',
                           'QVpVQ0VOQUFWSU5BSEVSTkFOREVaMjgzM0hJRERFTlZBTExFWUxOODY0MDRNT0hBVkU=',
                           'QU5BQkVMTFNBTkRFWjI4MTVDSVNDT0RSTjg2NDAzTU9IQVZF',
                           'QklMTFlEQVZJRENSWUU2NDRTRU1FUllQQVJLUkQ4NjQxMy03ODE0TU9IQVZF',
                           'SlVBTk1BTlVFTFZBWlFVRVoxNjUwSElHSExBTkRSRDg2NDQyLTcwNjZNT0hBVkU=',
                           'RklERUxBT0NIT0EyODM0U0hPT1BFU0FWRTg1NTUyR1JBSEFN',
                           'Sk9SR0VBTFZBUkVaMjk4NVBBTE9WRVJERUJMVkRTODY0MDRNT0hBVkU=',
                           'Sk9ITldJTExJQU1CQVRURVJUT04zMjU2U0hFSUtEUjg2NDA0LTE1NzNNT0hBVkU=',
                           'UkFZTU9OREpVTklPUkVTQ09CRURPMzYyNlRBUlBPTkRSODY0MDZNT0hBVkU=',
                           'Uk9CRVJUV0FZTkVKQUNLU09OMzI0OUVDSEVZRU5ORURSODY0NDBNT0hBVkU=',
                           'Uk9CRVJUQUxFWEFOREVSU0FCQVRBTk8zMzgzSElMTERBTEVEUjg2NDA2TU9IQVZF',
                           'SlVTVElDRUxBWU5FTkVMU09OOTYyNk5FREVOU1BSSU5HU1JEODU1MzVHUkFIQU0=',
                           'UklDS1lMQVdSRU5DRURPSFJOMzA0MVdJTlRFUkhBVkVORFI4NjQwNE1PSEFWRQ==',
                           'QURBTkdBUkNJQTE3ODRTRUFDUkVFS0RSODY0NDJNT0hBVkU=',
                           'S0VOTkVUSFNDT1RUWldJQ0tMMjE0NlNBTkpVQU5EUjg2NDAzLTI5NDFNT0hBVkU='
    )