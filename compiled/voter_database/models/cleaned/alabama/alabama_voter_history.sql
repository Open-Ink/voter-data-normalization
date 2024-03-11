WITH ALABAMA_ELECTION_HISTORY
         AS (SELECT DB_LOGID             as county,
                    TEXT_VOTER_ID        as voter_id,
                    TEXT_ELECTION_CODE_1 as election_code,
                    PARTY_VOTED_1        as party_voted
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_1 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_2, PARTY_VOTED_2
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_2 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_3, PARTY_VOTED_3
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_3 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_4, PARTY_VOTED_4
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_4 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_5, PARTY_VOTED_5
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_5 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_6, PARTY_VOTED_6
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_6 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_7, PARTY_VOTED_7
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_7 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_8, PARTY_VOTED_8
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_8 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_9, PARTY_VOTED_9
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_9 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID, TEXT_VOTER_ID, TEXT_ELECTION_CODE_10, PARTY_VOTED_10
             FROM DBT_VOTER_DATA.raw.ALABAMA_VOTERS_2024_01_15
             WHERE TEXT_ELECTION_CODE_10 IS NOT NULL)
SELECT 'AL'                                       as STATE_CODE,
       REPLACE(VH.county, 'ST_CLAIR', 'St Clair') AS COUNTY_CODE,
       VH.voter_id                                AS VOTER_ID,
       E.election_name                            AS ELECTION_TYPE,
       TO_DATE(E.election_date, 'MM/DD/YYYY')     AS ELECTION_DATE,
       VH.party_voted                             AS PARTY_VOTED,
       'UNKNOWN'                                  AS VOTING_METHOD,
       NULL                                       AS DATE_OF_VOTING,
       TRUE                                       AS WAS_VOTE_COUNTED,
       NULL::object                               AS ADDITIONAL_DATA
FROM ALABAMA_ELECTION_HISTORY VH
         INNER JOIN DBT_VOTER_DATA.cleaned.alabama_elections E
                    ON VH.election_code = E.election_code