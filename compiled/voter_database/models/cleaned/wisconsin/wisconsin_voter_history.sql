with __dbt__cte__wisconsin_elections as (


SELECT *
FROM (VALUES ('2022-11', '2022-11-08', 'GENERAL'),
             ('2022-08', '2022-08-09', 'PRIMARY'),
             ('2020-11', '2020-11-03', 'GENERAL'),
             ('2020-08', '2020-08-11', 'PRIMARY'),
             ('2020-04', '2020-04-07', 'PRESIDENTIAL PRIMARY'),
             ('2018-11', '2018-11-06', 'GENERAL'),
             ('2018-08', '2018-08-14', 'PRIMARY'),
             ('2016-11', '2016-11-08', 'GENERAL'),
             ('2016-08', '2016-08-09', 'PRIMARY'),
             ('2016-04', '2016-04-05', 'PRESIDENTIAL PRIMARY'),
             ('2014-11', '2014-11-04', 'GENERAL'),
             ('2014-08', '2014-08-12', 'PRIMARY'),
             ('2012-11', '2012-11-06', 'GENERAL'),
             ('2012-08', '2012-08-14', 'PRIMARY'),
             ('2012-03', '2012-03-3', 'PRESIDENTIAL PRIMARY'),
             ('2010-11', '2010-11-2', 'GENERAL'),
             ('2010-09', '2010-09-14', 'PRIMARY'),
             ('2008-11', '2008-11-4', 'GENERAl'),
             ('2008-09', '2008-09-9', 'PRIMARY'),
             ('2008-02', '2008-02-19', 'PRESIDENTIAL PRIMARY'),
             ('2006-11', '2006-11-7', 'GENERAL')
         ) AS A (ELECTION_MONTH_AND_YEAR, ELECTION_DATE, ELECTION_TYPE)
) SELECT 'WI'             AS STATE_CODE,
       REPLACE(
               REPLACE(VH.COUNTY, ' County', ''),
               'St.',
               'Saint') AS COUNTY_CODE,
       VOTER_ID         AS VOTER_ID,
       E.ELECTION_TYPE  AS ELECTION_TYPE,
       E.ELECTION_DATE  AS ELECTION_DATE,
       PARTY            AS PARTY_VOTED,
       CASE
           WHEN VOTE_METHOD IN ('At Polls') THEN 'STANDARD'
           WHEN VOTE_METHOD IN ('Absentee') THEN 'ABSENTEE'
           ELSE 'Unknown'
           END          AS VOTING_METHOD,
       NULL             AS DATE_OF_VOTING,
       TRUE             AS WAS_VOTE_COUNTED,
       NULL::object     AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.WISCONSIN_VOTER_HISTORY_2023_10_17 VH
         INNER JOIN __dbt__cte__wisconsin_elections E ON VH.ELECTION_DATE = E.ELECTION_MONTH_AND_YEAR