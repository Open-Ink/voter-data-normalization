WITH  __dbt__cte__nebraska_elections as (


SELECT *
FROM (VALUES ('GN22', 'GENERAL', '11/8/2022'),
             ('PR22', 'PRIMARY', '5/10/2022'),
             ('GN20', 'GENERAL', '11/3/2020'),
             ('PR20', 'PRIMARY', '5/12/2020'),
             ('GN18', 'GENERAL', '11/6/2018'),
             ('PR18', 'PRIMARY', '5/15/2018'),
             ('GN16', 'GENERAL', '11/8/2016'),
             ('PR 16', 'PRIMARY', '5/10/2016'),
             ('GN14', 'GENERAL', '11/4/2014'),
             ('PR14', 'PRIMARY', '5/13/2014'),
             ('GN12', 'GENERAL', '11/6/2012'),
             ('PR12', 'PRIMARY', '5/15/2012'),
             ('GN10', 'GENERAL', '11/2/2010'),
             ('PR10', 'PRIMARY', '5/11/2010'),
             ('GN08', 'GENERAL', '11/4/2008'),
             ('PR08', 'PRIMARY', '5/13/2008'),
             ('GN06', 'GENERAL', '11/7/2006'),
             ('PR06', 'PRIMARY', '5/9/2006'))
         AS A(ELECTION_CODE, ELECTION_TYPE, ELECTION_DATE)
), NEBRASKA_HISTORY
         AS (SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_1 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_1 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_2 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_2 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_3 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_3 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_4 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_4 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_5 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_5 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_6 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_6 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_7 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_7 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_8 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_8 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_9 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_9 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.NEBRASKA_VOTERS_2023_11_01 V
                      INNER JOIN __dbt__cte__nebraska_elections E
                                 ON V.TEXT_ELECTION_CODE_10 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_10 IS NOT NULL)
SELECT 'NE'                                 AS STATE_CODE,
       COUNTY_CODE                          AS COUNTY_CODE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       VOTER_ID                             AS VOTER_ID,
       ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                 AS PARTY_VOTED,
       'UNKNOWN'                            AS VOTING_METHOD,
       NULL                                 AS DATE_OF_VOTING,
       TRUE                                 AS WAS_VOTE_COUNTED,
       NULL::object                         AS ADDITIONAL_DATA
FROM NEBRASKA_HISTORY