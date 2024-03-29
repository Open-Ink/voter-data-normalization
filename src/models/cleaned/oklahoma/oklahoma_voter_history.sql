WITH OK_ELECTIONS AS (SELECT *
                      FROM (
                               VALUES ('03/07/2023', 'SPECIAL'),
                                      ('11/08/2022', 'GENERAL'),
                                      ('08/23/2022', 'PRIMARY RUNOFF'),
                                      ('06/28/2022', 'PRIMARY'),
                                      ('11/03/2020', 'GENERAL'),
                                      ('08/25/2020', 'PRIMARY RUNOFF'),
                                      ('06/30/2020', 'PRIMARY'),
                                      ('03/03/2020', 'PRESIDENTIAL PRIMARY'),
                                      ('11/06/2018', 'GENERAL'),
                                      ('08/28/2018', 'PRIMARY RUNOFF'),
                                      ('06/26/2018', 'PRIMARY'),
                                      ('11/08/2016', 'GENERAL'),
                                      ('08/23/2016', 'PRIMARY RUNOFF'),
                                      ('06/28/2016', 'PRIMARY'),
                                      ('03/01/2016', 'PRESIDENTIAL PRIMARY'),
                                      ('11/04/2014', 'GENERAL'),
                                      ('08/26/2014', 'PRIMARY RUNOFF'),
                                      ('06/24/2014', 'PRIMARY'),
                                      ('11/06/2012', 'GENERAL'),
                                      ('08/28/2012', 'PRIMARY RUNOFF'),
                                      ('06/26/2012', 'PRIMARY'),
                                      ('03/06/2012', 'PRESIDENTIAL PRIMARY'),
                                      ('11/02/2010', 'GENERAL'),
                                      ('08/24/2010', 'PRIMARY RUNOFF'),
                                      ('07/27/2010', 'PRIMARY'),
                                      ('11/04/2008', 'GENERAL'),
                                      ('08/26/2008', 'PRIMARY RUNOFF'),
                                      ('07/29/2008', 'PRIMARY'),
                                      ('02/05/2008', 'PRESIDENTIAL PRIMARY'),
                                      ('11/07/2006', 'GENERAL'),
                                      ('08/22/2006', 'PRIMARY RUNOFF'),
                                      ('07/25/2006', 'PRIMARY'),
                                      ('11/02/2004', 'GENERAL'),
                                      ('08/24/2004', 'PRIMARY RUNOFF'),
                                      ('07/27/2004', 'PRIMARY'),
                                      ('02/03/2004', 'PRESIDENTIAL PRIMARY'),
                                      ('11/05/2002', 'GENERAL'),
                                      ('08/27/2002', 'PRIMARY RUNOFF'),
                                      ('11/07/2000', 'GENERAL'),
                                      ('08/22/2000', 'PRIMARY RUNOFF'),
                                      ('03/14/2000', 'PRESIDENTIAL PRIMARY')
                               ) AS A (ELECTION_DATE, ELECTION_TYPE))
SELECT 'OK'                                        AS STATE_CODE,
       REPLACE(V.COUNTY_DESC, 'LEFLORE', 'Le Flore') AS COUNTY_CODE,
       V.VOTER_ID                                  AS VOTER_ID,
       ELE.ELECTION_TYPE                           AS ELECTION_TYPE,
       ELE.ELECTION_DATE                           AS ELECTION_DATE,
       CASE
           WHEN VH.VOTING_METHOD = 'IP' THEN 'STANDARD'
           WHEN VH.VOTING_METHOD = 'AB' THEN 'ABSENTEE'
           ELSE VH.VOTING_METHOD END               AS VOTING_METHOD,
       NULL                                        AS DATE_OF_VOTING,
       TRUE                                        AS WAS_VOTE_COUNTED,
       NULL::object                                AS ADDITIONAL_DATA,
       NULL                                        AS PARTY_VOTED
FROM {{ source('raw', 'OKLAHOMA_VOTER_HISTORY_2023_09_23') }} VH
         INNER JOIN OK_ELECTIONS ELE ON VH.ELECTION_DATE = ELE.ELECTION_DATE
         INNER JOIN {{ source('raw', 'OKLAHOMA_VOTERS_2023_09_23') }} V ON VH.VOTER_ID = V.VOTER_ID