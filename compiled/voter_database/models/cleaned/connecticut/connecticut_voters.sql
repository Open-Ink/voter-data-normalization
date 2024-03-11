SELECT DISTINCT '2023-10-12'::date                  as FILE_DATE,
                'CT'                                as STATE_CODE,
                T.COUNTY_NAME                       as COUNTY_CODE,
                WS_ID_VOTER                         AS VOTER_ID,
                WS_VTR_NM_PREFIX                    AS PREFIX,
                WS_VTR_NM_FIRST                     AS FIRST_NAME,
                WS_VTR_NM_MID                       AS MIDDLE_NAME,
                WS_VTR_NM_LAST                      AS LAST_NAME,
                WS_VTR_NM_SUFF                      AS NAME_SUFFIX,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([
                    WS_VTR_AD_NUM,
                    WS_NM_STREET
                    ])                              AS RESIDENCE_ADDRESS_LINE_1,
                WS_VTR_AD_UNIT                      AS RESIDENCE_ADDRESS_LINE_2,
                WS_TOWN_NAME                        AS RESIDENCE_ADDRESS_CITY,
                COALESCE(WS_ST, 'CT')               AS RESIDENCE_ADDRESS_STATE,
                WS_ZIP5                             AS RESIDENCE_ADDRESS_ZIPCODE,
                'USA'                               AS RESIDENCE_ADDRESS_COUNTRY,
                WS_DT_BIRTH_CCYY::number            AS BIRTH_YEAR,
                WS_DT_BIRTH_MM::number              AS BIRTH_MONTH,
                NULL                                AS BIRTH_DATE,
                WS_VTR_CD_STATUS                    as VOTER_STATUS,
                TO_DATE(WS_DT_ACCEPT, 'MM/DD/YYYY') AS REGISTRATION_DATE,
                NULL                                AS LAST_VOTED_DATE,
                WS_CD_PARTY                         AS LAST_PARTY_VOTED,
                WS_CONGRESS                         AS CONGRESSIONAL_DISTRICT,
                WS_SENATE                           AS STATE_SENATE_DISTRICT,
                WS_ASSEMBLY                         AS STATE_HOUSE_DISTRICT,
                NULL                                AS JUDICIAL_DISTRICT,
                NULL                                AS COUNTY_COMMISSION_DISTRICT,
                NULL                                AS SCHOOL_BOARD_DISTRICT,
                NULL                                AS CITY_COUNCIL_DISTRICT,
                WS_VTR_PREC                         AS COUNTY_PRECINCT,
                NULL                                AS MUNICIPAL_PRECINCT,
                NULL                                AS RACE,
                NULL                                AS GENDER,
                MERGE_ADDRESS_ELEMENTS([
                    WS_MAIL_NUM,
                    WS_MAIL_STR1
                    ])                              AS MAILING_ADDRESS_LINE_1,
                MERGE_ADDRESS_ELEMENTS([
                    WS_MAIL_UNIT,
                    WS_MAIL_STR2
                    ])                              AS MAILING_ADDRESS_LINE_2,
                NULL                                AS MAILING_LINE_3,
                WS_MAIL_CITY                        AS MAILING_CITY,
                WS_MAIL_ST                          AS MAILING_STATE,
                WS_MAIL_ZIP                         AS MAILING_ZIPCODE,
                WS_MAIL_COUNTRY                     AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12 VOTERS
         INNER JOIN DBT_VOTER_DATA.cleaned.connecticut_towns T ON VOTERS.WS_TOWN_ID = T.TOWN_ID
WHERE TRY_TO_DATE(WS_DT_ACCEPT, 'MM/DD/YYYY') IS NOT NULL