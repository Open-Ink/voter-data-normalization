with __dbt__cte__florida_counties as (


SELECT *
FROM (VALUES ('ALA', 'Alachua'),
             ('HAM', 'Hamilton'),
             ('OKE', 'Okeechobee'),
             ('BAK', 'Baker'),
             ('HAR', 'Hardee'),
             ('ORA', 'Orange'),
             ('BAY', 'Bay'),
             ('HEN', 'Hendry'),
             ('OSC', 'Osceola'),
             ('BRA', 'Bradford'),
             ('HER', 'Hernando'),
             ('PAL', 'Palm Beach'),
             ('BRE', 'Brevard'),
             ('HIG', 'Highlands'),
             ('PAS', 'Pasco'),
             ('BRO', 'Broward'),
             ('HIL', 'Hillsborough'),
             ('PIN', 'Pinellas'),
             ('CAL', 'Calhoun'),
             ('HOL', 'Holmes'),
             ('POL', 'Polk'),
             ('CHA', 'Charlotte'),
             ('IND', 'Indian River'),
             ('PUT', 'Putnam'),
             ('CIT', 'Citrus'),
             ('JAC', 'Jackson'),
             ('SAN', 'Santa Rosa'),
             ('CLA', 'Clay'),
             ('JEF', 'Jefferson'),
             ('SAR', 'Sarasota'),
             ('CLL', 'Collier'),
             ('LAF', 'Lafayette'),
             ('SEM', 'Seminole'),
             ('CLM', 'Columbia'),
             ('LAK', 'Lake'),
             ('STJ', 'Saint Johns'),
             ('DAD', 'Miami-Dade'),
             ('LEE', 'Lee'),
             ('STL', 'Saint Lucie'),
             ('DES', 'Desoto'),
             ('LEO', 'Leon'),
             ('SUM', 'Sumter'),
             ('DIX', 'Dixie'),
             ('LEV', 'Levy'),
             ('SUW', 'Suwannee'),
             ('DUV', 'Duval'),
             ('LIB', 'Liberty'),
             ('TAY', 'Taylor'),
             ('ESC', 'Escambia'),
             ('MAD', 'Madison'),
             ('UNI', 'Union'),
             ('FLA', 'Flagler'),
             ('MAN', 'Manatee'),
             ('VOL', 'Volusia'),
             ('FRA', 'Franklin'),
             ('MRN', 'Marion'),
             ('WAK', 'Wakulla'),
             ('GAD', 'Gadsden'),
             ('MRT', 'Martin'),
             ('WAL', 'Walton'),
             ('GIL', 'Gilchrist'),
             ('MON', 'Monroe'),
             ('WAS', 'Washington'),
             ('GLA', 'Glades'),
             ('NAS', 'Nassau'),
             ('GUL', 'Gulf'),
             ('OKA', 'Okaloosa'))
         AS A (CODE, NAME)
) SELECT 'FL'                                            AS STATE_CODE,
       C.NAME                                          AS COUNTY_CODE,
       VOTER_ID                                        AS VOTER_ID,
       CASE
           WHEN ELECTION_TYPE = 'GEN' THEN 'GENERAL'
           WHEN ELECTION_TYPE = 'PRI' THEN 'GENERAL PRIMARY'
           WHEN ELECTION_TYPE = 'PPP' THEN 'PPP'
           WHEN ELECTION_TYPE = 'RUN' THEN 'GENERAL RUNOFFS'
           WHEN ELECTION_TYPE = 'OTH' THEN 'OTHER' END AS ELECTION_TYPE,
       ELECTION_DATE::date                             AS ELECTION_DATE,
       NULL                                            AS PARTY_VOTED,
       CASE
           WHEN HISTORY_CODE IN ('A', 'B') THEN 'ABSENTEE'
           WHEN HISTORY_CODE IN ('E') THEN 'EARLY_VOTING'
           WHEN HISTORY_CODE IN ('Y') THEN 'STANDARD'
           ELSE 'STANDARD'
           END                                         AS VOTING_METHOD,
       NULL                                            AS DATE_OF_VOTING,
       CASE
           WHEN HISTORY_CODE IN ('B', 'P')
               THEN FALSE
           ELSE TRUE END                               AS WAS_VOTE_COUNTED,
       NULL::object                                    AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.FLORIDA_VOTER_HISTORY_2023_08_08 V
         LEFT JOIN __dbt__cte__florida_counties C
                   ON REPLACE(V.COUNTY_CODE, '.', '') = C.CODE