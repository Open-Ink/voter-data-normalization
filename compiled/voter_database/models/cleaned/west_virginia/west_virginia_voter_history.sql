SELECT 'WV'                                                      AS STATE_CODE,
       V.COUNTY_CODE                                             AS COUNTY_CODE,
       VH.ID_VOTER                                               AS VOTER_ID,
       CASE
           WHEN NM_ELECTION ILIKE '%GENERAL%' THEN 'GENERAL'
           WHEN NM_ELECTION ILIKE '%PRIMARY%' THEN 'PRIMARY' END AS ELECTION_TYPE,
       DT_ELECTION::date                                         AS ELECTION_DATE,
       NULL                                                      AS PARTY_VOTED,
       CASE
           WHEN CD_ENTRY_TYPE = 'A' THEN 'ABSENTEE'
           WHEN CD_ENTRY_TYPE = 'UE' THEN 'EARLY_VOTING'
           WHEN CD_ENTRY_TYPE = 'R' THEN 'REGULAR'
           WHEN CD_ENTRY_TYPE = 'E' THEN 'EARLY_VOTING' END      AS VOTING_METHOD,
       NULL                                                      AS DATE_OF_VOTING,
       TRUE                                                      AS WAS_VOTE_COUNTED,
       NULL::object                                              AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.WEST_VIRGINIA_VOTER_HISTORY VH
         INNER JOIN DBT_VOTER_DATA.cleaned.west_virginia_voters V ON V.VOTER_ID = VH.ID_VOTER
WHERE CD_ELECTION_CAT = 'SW'