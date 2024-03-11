WITH  __dbt__cte__tennessee_counties as (


SELECT *
FROM (VALUES ('01', 'ANDERSON', '1'),
             ('02', 'BEDFORD', '2'),
             ('08', 'CANNON', '8'),
             ('03', 'BENTON', '3'),
             ('04', 'BLEDSOE', '4'),
             ('06', 'BRADLEY', '6'),
             ('05', 'BLOUNT', '5'),
             ('07', 'CAMPBELL', '7'),
             ('09', 'CARROLL', '9'),
             ('10', 'CARTER', '10'),
             ('11', 'CHEATHAM', '11'),
             ('12', 'CHESTER', '12'),
             ('13', 'CLAIBORNE', '13'),
             ('14', 'CLAY', '14'),
             ('15', 'COCKE', '15'),
             ('16', 'COFFEE', '16'),
             ('17', 'CROCKETT', '17'),
             ('19', 'DAVIDSON', '19'),
             ('18', 'CUMBERLAND', '18'),
             ('20', 'DECATUR', '20'),
             ('21', 'DEKALB', '21'),
             ('22', 'DICKSON', '22'),
             ('23', 'DYER', '23'),
             ('24', 'FAYETTE', '24'),
             ('25', 'FENTRESS', '25'),
             ('26', 'FRANKLIN', '26'),
             ('27', 'GIBSON', '27'),
             ('28', 'GILES', '28'),
             ('37', 'HAWKINS', '37'),
             ('36', 'HARDIN', '36'),
             ('29', 'GRAINGER', '29'),
             ('35', 'HARDEMAN', '35'),
             ('38', 'HAYWOOD', '38'),
             ('34', 'HANCOCK', '34'),
             ('33', 'HAMILTON', '33'),
             ('32', 'HAMBLEN', '32'),
             ('31', 'GRUNDY', '31'),
             ('30', 'GREENE', '30'),
             ('42', 'HOUSTON', '42'),
             ('43', 'HUMPHREYS', '43'),
             ('39', 'HENDERSON', '39'),
             ('40', 'HENRY', '40'),
             ('44', 'JACKSON', '44'),
             ('41', 'HICKMAN', '41'),
             ('45', 'JEFFERSON', '45'),
             ('46', 'JOHNSON', '46'),
             ('47', 'KNOX', '47'),
             ('53', 'LOUDON', '53'),
             ('52', 'LINCOLN', '52'),
             ('51', 'LEWIS', '51'),
             ('48', 'LAKE', '48'),
             ('50', 'LAWRENCE', '50'),
             ('54', 'MCMINN', '54'),
             ('49', 'LAUDERDALE', '49'),
             ('70', 'POLK', '70'),
             ('69', 'PICKETT', '69'),
             ('68', 'PERRY', '68'),
             ('67', 'OVERTON', '67'),
             ('66', 'OBION', '66'),
             ('64', 'MOORE', '64'),
             ('63', 'MONTGOMERY', '63'),
             ('71', 'PUTNAM', '71'),
             ('56', 'MACON', '56'),
             ('62', 'MONROE', '62'),
             ('61', 'MEIGS', '61'),
             ('55', 'MCNAIRY', '55'),
             ('60', 'MAURY', '60'),
             ('65', 'MORGAN', '65'),
             ('59', 'MARSHALL', '59'),
             ('58', 'MARION', '58'),
             ('57', 'MADISON', '57'),
             ('72', 'RHEA', '72'),
             ('74', 'ROBERTSON', '74'),
             ('75', 'RUTHERFORD', '75'),
             ('73', 'ROANE', '73'),
             ('76', 'SCOTT', '76'),
             ('77', 'SEQUATCHIE', '77'),
             ('88', 'VAN BUREN', '88'),
             ('89', 'WARREN', '89'),
             ('87', 'UNION', '87'),
             ('86', 'UNICOI', '86'),
             ('85', 'TROUSDALE', '85'),
             ('84', 'TIPTON', '84'),
             ('78', 'SEVIER', '78'),
             ('83', 'SUMNER', '83'),
             ('82', 'SULLIVAN', '82'),
             ('81', 'STEWART', '81'),
             ('79', 'SHELBY', '79'),
             ('80', 'SMITH', '80'),
             ('90', 'WASHINGTON', '90'),
             ('94', 'WILLIAMSON', '94'),
             ('93', 'WHITE', '93'),
             ('95', 'WILSON', '95'),
             ('91', 'WAYNE', '91'),
             ('92', 'WEAKLEY', '92')) AS A (COUNTY_ID, COUNTY_NAME, COUNTY_ID_INT)
), VOTER_HISTORY AS
         (SELECT COUNTY,
                 VOTER_ID,
                 NOV_22_HOW                   as VOTING_METHOD,
                 NULL                         AS PARTY_VOTED,
                 'GENERAL'                    as ELECTION_TYPE,
                 NEXT_DAY('2022-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_22_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_22_HOW,
                 AUG_22_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2022-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_22_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_20_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2020-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_20_HOW,
                 AUG_20_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2020-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_20_HOW,
                 MAR_20_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2020-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE MAR_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_18_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2018-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_18_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_18_HOW,
                 AUG_18_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2018-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_16_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2016-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_16_HOW,
                 AUG_16_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2016-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_16_HOW,
                 MAR_16_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2016-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE MAR_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_14_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2014-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_14_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_14_HOW,
                 AUG_14_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2014-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_12_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2012-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_12_HOW,
                 AUG_12_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2012-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_12_HOW,
                 MAR_12_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2012-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE MAR_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_10_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2010-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_10_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_10_HOW,
                 AUG_10_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2010-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_10_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_08_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2008-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_08_HOW,
                 AUG_08_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2008-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 FEB_08_HOW,
                 FEB_08_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2008-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE FEB_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_06_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2006-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_06_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_06_HOW,
                 AUG_06_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2006-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_06_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_04_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2004-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_04_HOW,
                 AUG_04_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2004-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 FEB_04_HOW,
                 FEB_04_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2004-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE FEB_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_02_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2002-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_02_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_02_HOW,
                 AUG_02_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2002-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_02_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_00_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2000-11-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE NOV_00_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_00_HOW,
                 AUG_00_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2000-08-01', 'th') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE AUG_00_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_00_HOW,
                 MAR_00_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2000-03-01', 'tu') AS ELECTION_DATE
          FROM DBT_VOTER_DATA.raw.TENNESSEE_VOTERS_2024_01_01
          WHERE MAR_00_HOW IS NOT NULL)
SELECT 'TN'             AS STATE_CODE,
       C.COUNTY_NAME        AS COUNTY_CODE,
       VH.VOTER_ID      AS VOTER_ID,
       VH.ELECTION_DATE AS ELECTION_DATE,
       VH.PARTY_VOTED   AS PARTY_VOTED,
       NULL             AS VOTING_METHOD,
       NULL             AS DATE_OF_VOTING,
       TRUE             AS WAS_VOTE_COUNTED,
       NULL::object     AS ADDITIONAL_DATA,
       VH.ELECTION_TYPE AS ELECTION_TYPE
FROM VOTER_HISTORY VH
         INNER JOIN __dbt__cte__tennessee_counties C ON VH.COUNTY = C.COUNTY_ID