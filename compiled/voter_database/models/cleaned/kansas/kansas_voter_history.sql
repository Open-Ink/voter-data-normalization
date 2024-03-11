WITH  __dbt__cte__kansas_elections as (


SELECT *
FROM (VALUES ('PR2022', 'PRIMARY', '8/2/2022'),
             ('GN2022', 'GENERAL', '11/8/2022'),
             ('PR2020', 'PRIMARY', '8/4/2022'),
             ('GN2020', 'GENERAL', '11/3/2020'),
             ('GN2018', 'GENERAL', '11/6/2018'),
             ('PR2018', 'PRIMARY', '8/7/2018'),
             ('PR2016', 'PRIMARY', '8/2/2016'),
             ('GN2016', 'GENERAL', '11/8/2016'),
             ('PR2014', 'PRIMARY', '8/5/2014'),
             ('GN2014', 'GENERAL', '11/4/2014'),
             ('PR2012', 'PRIMARY', '8/7/2012'),
             ('GN2012', 'GENERAL', '11/6/2012'),
             ('PR2010', 'PRIMARY', '8/3/2010'),
             ('GN2010', 'GENERAL', '11/9/2010'),
             ('GN2008', 'GENERAL', '11/4/2008'),
             ('PR2008', 'PRIMARY', '8/5/2008'),
             ('PR2007', 'PRIMARY', '8/7/2008'),
             ('GN2007', 'GENERAL', '11/6/2007'),
             ('GN2006', 'GENERAL', '11/7/2006'),
             ('PR2006', 'PRIMARY', '8/1/2006'),
             ('PR2004', 'PRIMARY', '8/3/2004'),
             ('GN2004', 'GENERAL', '11/9/2004'),
             ('PR2002', 'PRIMARY', '8/6/2002'),
             ('GN2002', 'GENERAL', '11/5/2002'),
             ('GN2000', 'GENERAL', '11/7/2000'),
             ('PR2000', 'PRIMARY', '8/1/2000'),
             ('PR1998', 'PRIMARY', '8/4/1998'),
             ('GN1998', 'GENERAL', '11/3/1998'),
             ('PR1996', 'PRIMARY', '8/5/1996'),
             ('GN1996', 'GENERAL', '11/5/1996'))
         AS A(ELECTION_CODE, ELECTION_TYPE, ELECTION_DATE)
), VOTER_HISTORY AS (SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_1 AS TEXT_ELECTION_CODE
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_1 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_2
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_2 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_3
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_3 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_4
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_4 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_5
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_5 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_6
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_6 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_7
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_7 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_8
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_8 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_9
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_9 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_10
                       FROM DBT_VOTER_DATA.raw.KANSAS_VOTERS_2023_11_06
                       WHERE TEXT_ELECTION_CODE_10 IS NOT NULL)
SELECT 'KS'                                   AS STATE_CODE,
       DB_LOGID                               AS COUNTY_CODE,
       TO_DATE(E.ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       TEXT_REGISTRANT_ID                     AS VOTER_ID,
       E.ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                   AS PARTY_VOTED,
       'UNKNOWN'                              AS VOTING_METHOD,
       NULL                                   AS DATE_OF_VOTING,
       TRUE                                   AS WAS_VOTE_COUNTED,
       NULL::object                           AS ADDITIONAL_DATA
FROM VOTER_HISTORY VH
         INNER JOIN __dbt__cte__kansas_elections E
                    ON VH.TEXT_ELECTION_CODE = E.ELECTION_CODE