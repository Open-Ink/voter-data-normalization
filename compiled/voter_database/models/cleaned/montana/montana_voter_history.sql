SELECT 'MT'                                      AS STATE_CODE,
       V.COUNTY_CODE                             AS COUNTY_CODE,
       VH.VOTER_ID                               AS VOTER_ID,
       ELECTION_TYPE                             AS ELECTION_TYPE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY')      AS ELECTION_DATE,
       NULL                                      AS PARTY_VOTED,
       CASE
           WHEN VVM_ID IN ('1, 4') THEN 'ABSENTEE'
           WHEN VVM_ID = '2' THEN 'STANDARD' END AS VOTING_METHOD,
       NULL                                      AS DATE_OF_VOTING,
       CASE
           WHEN BALLOTSTAGE_STATUS
               IN ('Rejected', 'Undeliverable', 'Received', 'Canceled')
               THEN FALSE
           ELSE TRUE END                         AS WAS_VOTE_COUNTED,
       IFF(VH.SENT_DATE IS NOT NULL, OBJECT_CONSTRUCT(
               'absentee_ballot_sent_date', VH.SENT_DATE,
               'absentee_ballot_received_date', VH.RECEIVE_DATE,
               'absentee_ballot_status', VH.BALLOTSTAGE_STATUS
                                     ), NULL)    AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.MONTANA_VOTER_HISTORY_2023_11_06 VH
         INNER JOIN DBT_VOTER_DATA.cleaned.montana_voters V
                    ON VH.VOTER_ID = V.VOTER_ID