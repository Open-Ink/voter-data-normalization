SELECT DISTINCT '2023-09-01'::date                          as FILE_DATE,
                'AR'                                        as STATE_CODE,
                VOTERS.COUNTY                               as COUNTY_CODE,
                VOTERS.VOTER_ID                             AS VOTER_ID,
                CDE_NAME_TITLE                              AS PREFIX,
                VOTERS.TEXT_NAME_FIRST                      AS FIRST_NAME,
                VOTERS.TEXT_NAME_MIDDLE                     AS MIDDLE_NAME,
                VOTERS.TEXT_NAME_LAST                       AS LAST_NAME,
                VOTERS.CDE_NAME_SUFFIX                      AS NAME_SUFFIX,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([
                    TEXT_RES_ADDRESS_NBR,
                    TEXT_RES_ADDRESS_NBR_SUFFIX,
                    CDE_STREET_DIR_PREFIX,
                    TEXT_STREET_NAME,
                    DESC_STREET_TYPE,
                    CDE_STREET_DIR_SUFFIX
                    ])                                      AS RESIDENCE_ADDRESS_LINE_1,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([
                    DESC_UNIT_TYPE,
                    TEXT_RES_UNIT_NBR
                    ])                                      AS RESIDENCE_ADDRESS_LINE_2,
                TEXT_RES_CITY                               AS RESIDENCE_ADDRESS_CITY,
                COALESCE(CDE_RES_STATE, 'AR')               AS RESIDENCE_ADDRESS_STATE,
                TEXT_RES_ZIP5                               AS RESIDENCE_ADDRESS_ZIPCODE,
                'USA'                                       AS RESIDENCE_ADDRESS_COUNTRY,
                YEAR(TO_DATE(DATE_OF_BIRTH))                AS BIRTH_YEAR,
                MONTH(TO_DATE(DATE_OF_BIRTH))               AS BIRTH_MONTH,
                DAYOFMONTH(TO_DATE(DATE_OF_BIRTH))          AS BIRTH_DATE,
                CASE
                    WHEN CDE_REGISTRANT_STATUS = 'A' THEN 'A'
                    WHEN CDE_REGISTRANT_STATUS = 'a' THEN 'A'
                    WHEN CDE_REGISTRANT_STATUS = 'I' THEN 'I'
                    ELSE CDE_REGISTRANT_STATUS
                    END                                     as VOTER_STATUS,
                TO_DATE(DATE_OF_REGISTRATION, 'MM/DD/YYYY') AS REGISTRATION_DATE,
                TO_DATE(DATE_LAST_VOTED, 'MM/DD/YYYY')      AS LAST_VOTED_DATE,
                CDE_PARTY                                   AS LAST_PARTY_VOTED,
                VOTERS.CONGRESSIONAL_DISTRICT               AS CONGRESSIONAL_DISTRICT,
                VOTERS.STATE_SENATE_DISTRICT                AS STATE_SENATE_DISTRICT,
                STATE_REPRESENTATIVE_DISTRICT               AS STATE_HOUSE_DISTRICT,
                VOTERS.JUDICIAL_DISTRICT                    AS JUDICIAL_DISTRICT,
                NULL                                        AS COUNTY_COMMISSION_DISTRICT,
                SCHOOL_DISTRICT                             AS SCHOOL_BOARD_DISTRICT,
                NULL                                        AS CITY_COUNCIL_DISTRICT,
                PRECINCT_NAME                               AS COUNTY_PRECINCT,
                NULL                                        AS MUNICIPAL_PRECINCT,
                NULL                                        AS RACE,
                NULL                                        AS GENDER,
                TEXT_MAIL_ADDRESS1                          AS MAILING_ADDRESS_LINE_1,
                TEXT_MAIL_ADDRESS2                          AS MAILING_ADDRESS_LINE_2,
                TEXT_MAIL_ADDRESS3                          AS MAILING_LINE_3,
                TEXT_MAIL_CITY                              AS MAILING_CITY,
                CDE_MAIL_STATE                              AS MAILING_STATE,
                TEXT_MAIL_ZIP5                              AS MAILING_ZIPCODE,
                TEXT_MAIL_ADDRESS4                          AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTERS_2023_09_01 VOTERS