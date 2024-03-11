SELECT 'MD'                                                       AS STATE_CODE,
       REPLACE(REPLACE(COUNTY_NAME, ' City', ''), ' Saint', 'St') AS COUNTY_CODE,
       VOTER_ID                                                   AS VOTER_ID,
       ELECTION_TYPE,
       ELECTION_DATE::date                                        AS ELECTION_DATE,
       POLITICAL_PARTY                                            AS PARTY_VOTED,
       CASE
           WHEN VOTING_METHOD IN ('EARLY VOTING') THEN 'EARLY_VOTING'
           WHEN VOTING_METHOD IN ('ABSENTEE') THEN 'ABSENTEE'
           WHEN VOTING_METHOD IN ('PROVISIONAL') THEN 'PROVISIONAL'
           WHEN VOTING_METHOD IN ('FWAB') THEN 'FWAB'
           ELSE 'STANDARD'
           END                                                    AS VOTING_METHOD,
       NULL                                                       AS DATE_OF_VOTING,
       TRUE                                                       AS WAS_VOTE_COUNTED,
       NULL::object                                               AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.MARYLAND_VOTER_HISTORY_2023_12_05