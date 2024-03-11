WITH WASHINGTON_HISTORY
         AS (SELECT 'WA'                                AS STATE_CODE,
                    STATEVOTERID                        AS VOTER_ID,
                    TO_DATE(ELECTIONDATE, 'YYYY-MM-DD') AS ELECTION_DATE
             FROM DBT_VOTER_DATA.raw.WASHINGTON_VOTER_HISTORY_2023_09_01 VH
             WHERE ELECTIONDATE IN ('2020-03-10',
                                    '2020-08-04',
                                    '2020-11-03',
                                    '2021-08-03',
                                    '2021-11-02',
                                    '2022-08-02',
                                    '2022-11-08',
                                    '2023-04-25',
                                    '2023-08-01'))
SELECT 'WA'                   AS STATE_CODE,
       V.COUNTY_CODE          AS COUNTY_CODE,
       VH.VOTER_ID            AS VOTER_ID,
       ELECTION_DATE          AS ELECTION_DATE,
       NULL                   AS PARTY_VOTED,
       NULL                   AS VOTING_METHOD,
       NULL                   AS DATE_OF_VOTING,
       TRUE                   AS WAS_VOTE_COUNTED,
       NULL::object           AS ADDITIONAL_DATA,
       CASE
           WHEN ELECTION_DATE = '2020-03-10' THEN 'PRESIDENTIAL_PRIMARY'
           WHEN ELECTION_DATE in ('2020-11-03', '2021-11-02', '2022-11-08') THEN 'GENERAL'
           WHEN ELECTION_DATE IN ('2020-08-04', '2021-08-03', '2022-08-02', '2023-08-01') THEN 'PRIMARY'
           ELSE 'UNKNOWN' END AS ELECTION_TYPE
FROM WASHINGTON_HISTORY VH
         INNER JOIN DBT_VOTER_DATA.cleaned.washington_voters V ON V.VOTER_ID = VH.VOTER_ID

