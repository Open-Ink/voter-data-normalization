with __dbt__cte__texas_counties as (


SELECT *
FROM (VALUES ('001', 'ANDERSON'),
             ('002', 'ANDREWS'),
             ('003', 'ANGELINA'),
             ('004', 'ARANSAS'),
             ('005', 'ARCHER'),
             ('006', 'ARMSTRONG'),
             ('007', 'ATASCOSA'),
             ('008', 'AUSTIN'),
             ('009', 'BAILEY'),
             ('010', 'BANDERA'),
             ('011', 'BASTROP'),
             ('012', 'BAYLOR'),
             ('013', 'BEE'),
             ('014', 'BELL'),
             ('015', 'BEXAR'),
             ('016', 'BLANCO'),
             ('017', 'BORDEN'),
             ('018', 'BOSQUE'),
             ('019', 'BOWIE'),
             ('020', 'BRAZORIA'),
             ('021', 'BRAZOS'),
             ('022', 'BREWSTER'),
             ('023', 'BRISCOE'),
             ('024', 'BROOKS'),
             ('025', 'BROWN'),
             ('026', 'BURLESON'),
             ('027', 'BURNET'),
             ('028', 'CALDWELL'),
             ('029', 'CALHOUN'),
             ('030', 'CALLAHAN'),
             ('031', 'CAMERON'),
             ('032', 'CAMP'),
             ('033', 'CARSON'),
             ('034', 'CASS'),
             ('035', 'CASTRO'),
             ('036', 'CHAMBERS'),
             ('037', 'CHEROKEE'),
             ('038', 'CHILDRESS'),
             ('039', 'CLAY'),
             ('040', 'COCHRAN'),
             ('041', 'COKE'),
             ('042', 'COLEMAN'),
             ('043', 'COLLIN'),
             ('044', 'COLLINGSWORTH'),
             ('045', 'COLORADO'),
             ('046', 'COMAL'),
             ('047', 'COMANCHE'),
             ('048', 'CONCHO'),
             ('049', 'COOKE'),
             ('050', 'CORYELL'),
             ('051', 'COTTLE'),
             ('052', 'CRANE'),
             ('053', 'CROCKETT'),
             ('054', 'CROSBY'),
             ('055', 'CULBERSON'),
             ('056', 'DALLAM'),
             ('057', 'DALLAS'),
             ('058', 'DAWSON'),
             ('059', 'DEAF SMITH'),
             ('060', 'DELTA'),
             ('061', 'DENTON'),
             ('062', 'DEWITT'),
             ('063', 'DICKENS'),
             ('064', 'DIMMIT'),
             ('065', 'DONLEY'),
             ('066', 'DUVAL'),
             ('067', 'EASTLAND'),
             ('068', 'ECTOR'),
             ('069', 'EDWARDS'),
             ('070', 'EL PASO'),
             ('071', 'ELLIS'),
             ('072', 'ERATH'),
             ('073', 'FALLS'),
             ('074', 'FANNIN'),
             ('075', 'FAYETTE'),
             ('076', 'FISHER'),
             ('077', 'FLOYD'),
             ('078', 'FOARD'),
             ('079', 'FORT BEND'),
             ('080', 'FRANKLIN'),
             ('081', 'FREESTONE'),
             ('082', 'FRIO'),
             ('083', 'GAINES'),
             ('084', 'GALVESTON'),
             ('085', 'GARZA'),
             ('086', 'GILLESPIE'),
             ('087', 'GLASSCOCK'),
             ('088', 'GOLIAD'),
             ('089', 'GONZALES'),
             ('090', 'GRAY'),
             ('091', 'GRAYSON'),
             ('092', 'GREGG'),
             ('093', 'GRIMES'),
             ('094', 'GUADALUPE'),
             ('095', 'HALE'),
             ('096', 'HALL'),
             ('097', 'HAMILTON'),
             ('098', 'HANSFORD'),
             ('099', 'HARDEMAN'),
             ('100', 'HARDIN'),
             ('101', 'HARRIS'),
             ('102', 'HARRISON'),
             ('103', 'HARTLEY'),
             ('104', 'HASKELL'),
             ('105', 'HAYS'),
             ('106', 'HEMPHILL'),
             ('107', 'HENDERSON'),
             ('108', 'HIDALGO'),
             ('109', 'HILL'),
             ('110', 'HOCKLEY'),
             ('111', 'HOOD'),
             ('112', 'HOPKINS'),
             ('113', 'HOUSTON'),
             ('114', 'HOWARD'),
             ('115', 'HUDSPETH'),
             ('116', 'HUNT'),
             ('117', 'HUTCHINSON'),
             ('118', 'IRION'),
             ('119', 'JACK'),
             ('120', 'JACKSON'),
             ('121', 'JASPER'),
             ('122', 'JEFF DAVIS'),
             ('123', 'JEFFERSON'),
             ('124', 'JIM HOGG'),
             ('125', 'JIM WELLS'),
             ('126', 'JOHNSON'),
             ('127', 'JONES'),
             ('128', 'KARNES'),
             ('129', 'KAUFMAN'),
             ('130', 'KENDALL'),
             ('131', 'KENEDY'),
             ('132', 'KENT'),
             ('133', 'KERR'),
             ('134', 'KIMBLE'),
             ('135', 'KING'),
             ('136', 'KINNEY'),
             ('137', 'KLEBERG'),
             ('138', 'KNOX'),
             ('139', 'LA SALLE'),
             ('140', 'LAMAR'),
             ('141', 'LAMB'),
             ('142', 'LAMPASAS'),
             ('143', 'LAVACA'),
             ('144', 'LEE'),
             ('145', 'LEON'),
             ('146', 'LIBERTY'),
             ('147', 'LIMESTONE'),
             ('148', 'LIPSCOMB'),
             ('149', 'LIVE OAK'),
             ('150', 'LLANO'),
             ('151', 'LOVING'),
             ('152', 'LUBBOCK'),
             ('153', 'LYNN'),
             ('154', 'MADISON'),
             ('155', 'MARION'),
             ('156', 'MARTIN'),
             ('157', 'MASON'),
             ('158', 'MATAGORDA'),
             ('159', 'MAVERICK'),
             ('160', 'MCCULLOCH'),
             ('161', 'MCLENNAN'),
             ('162', 'MCMULLEN'),
             ('163', 'MEDINA'),
             ('164', 'MENARD'),
             ('165', 'MIDLAND'),
             ('166', 'MILAM'),
             ('167', 'MILLS'),
             ('168', 'MITCHELL'),
             ('169', 'MONTAGUE'),
             ('170', 'MONTGOMERY'),
             ('171', 'MOORE'),
             ('172', 'MORRIS'),
             ('173', 'MOTLEY'),
             ('174', 'NACOGDOCHES'),
             ('175', 'NAVARRO'),
             ('176', 'NEWTON'),
             ('177', 'NOLAN'),
             ('178', 'NUECES'),
             ('179', 'OCHILTREE'),
             ('180', 'OLDHAM'),
             ('181', 'ORANGE'),
             ('182', 'PALO PINTO'),
             ('183', 'PANOLA'),
             ('184', 'PARKER'),
             ('185', 'PARMER'),
             ('186', 'PECOS'),
             ('187', 'POLK'),
             ('188', 'POTTER'),
             ('189', 'PRESIDIO'),
             ('190', 'RAINS'),
             ('191', 'RANDALL'),
             ('192', 'REAGAN'),
             ('193', 'REAL'),
             ('194', 'RED RIVER'),
             ('195', 'REEVES'),
             ('196', 'REFUGIO'),
             ('197', 'ROBERTS'),
             ('198', 'ROBERTSON'),
             ('199', 'ROCKWALL'),
             ('200', 'RUNNELS'),
             ('201', 'RUSK'),
             ('202', 'SABINE'),
             ('203', 'SAN AUGUSTINE'),
             ('204', 'SAN JACINTO'),
             ('205', 'SAN PATRICIO'),
             ('206', 'SAN SABA'),
             ('207', 'SCHLEICHER'),
             ('208', 'SCURRY'),
             ('209', 'SHACKELFORD'),
             ('210', 'SHELBY'),
             ('211', 'SHERMAN'),
             ('212', 'SMITH'),
             ('213', 'SOMERVELL'),
             ('214', 'STARR'),
             ('215', 'STEPHENS'),
             ('216', 'STERLING'),
             ('217', 'STONEWALL'),
             ('218', 'SUTTON'),
             ('219', 'SWISHER'),
             ('220', 'TARRANT'),
             ('221', 'TAYLOR'),
             ('222', 'TERRELL'),
             ('223', 'TERRY'),
             ('224', 'THROCKMORTON'),
             ('225', 'TITUS'),
             ('226', 'TOM GREEN'),
             ('227', 'TRAVIS'),
             ('228', 'TRINITY'),
             ('229', 'TYLER'),
             ('230', 'UPSHUR'),
             ('231', 'UPTON'),
             ('232', 'UVALDE'),
             ('233', 'VAL VERDE'),
             ('234', 'VAN ZANDT'),
             ('235', 'VICTORIA'),
             ('236', 'WALKER'),
             ('237', 'WALLER'),
             ('238', 'WARD'),
             ('239', 'WASHINGTON'),
             ('240', 'WEBB'),
             ('241', 'WHARTON'),
             ('242', 'WHEELER'),
             ('243', 'WICHITA'),
             ('244', 'WILBARGER'),
             ('245', 'WILLACY'),
             ('246', 'WILLIAMSON'),
             ('247', 'WILSON'),
             ('248', 'WINKLER'),
             ('249', 'WISE'),
             ('250', 'WOOD'),
             ('251', 'YOAKUM'),
             ('252', 'YOUNG'),
             ('253', 'ZAPATA'),
             ('254', 'ZAVALA')
         ) AS A (COUNTY_ID,COUNTY_NAME)
) SELECT '2023-11-28 '::date                       as FILE_DATE,
       'TX'                                      as STATE_CODE,
       C.COUNTY_NAME                             as COUNTY_CODE,
       VUID                                         VOTER_ID,
       NULL                                      AS PREFIX,
       FIRST_NAME                                AS FIRST_NAME,
       MIDDLE_NAME                               AS MIDDLE_NAME,
       LASTNAME                                  AS LAST_NAME,
       SUFFIX                                    AS NAME_SUFFIX,
       MERGE_ADDRESS_ELEMENTS([
           PERM_HOUSE_NUMBER,
           PERM_DESIGNATOR,
           PERM_DIRECTIONAL_PREFIX,
           PERM_STREET_NAME,
           PERM_STREET_TYPE,
           PERM_DIRECTIONAL_SUFFIX
           ])                                    AS RESIDENCE_ADDRESS_LINE_1,
       MERGE_ADDRESS_ELEMENTS([
           PERM_UNIT_NUMBER,
           PERM_UNIT_TYPE
           ])                                    AS RESIDENCE_ADDRESS_LINE_2,
       PERM_CITY                                 AS RESIDENCE_ADDRESS_CITY,
       'TX'                                      AS RESIDENCE_ADDRESS_STATE,
       PERM_ZIP_CODE                             AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                     AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DOB, 'YYYYMMDD'))            AS BIRTH_YEAR,
       MONTH(TO_DATE(DOB, 'YYYYMMDD'))           AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DOB, 'YYYYMMDD'))      AS BIRTH_DATE,
       CASE STATUS_CODE
           WHEN 'V' THEN 'A'
           WHEN 'S' THEN 'I'
           WHEN 'C' THEN 'C'
           END                                   as VOTER_STATUS,
       TO_DATE(DATE_OF_REGISTRATION, 'YYYYMMDD') AS REGISTRATION_DATE,
       NULL                                      AS LAST_VOTED_DATE,
       NULL                                      AS LAST_PARTY_VOTED,
       NULL                                      AS CONGRESSIONAL_DISTRICT,
       NULL                                      AS STATE_SENATE_DISTRICT,
       NULL                                      AS STATE_HOUSE_DISTRICT,
       NULL                                      AS JUDICIAL_DISTRICT,
       NULL                                      AS COUNTY_COMMISSION_DISTRICT,
       NULL                                      AS SCHOOL_BOARD_DISTRICT,
       NULL                                      AS CITY_COUNCIL_DISTRICT,
       NULL                                      AS COUNTY_PRECINCT,
       NULL                                      AS MUNICIPAL_PRECINCT,
       NULL                                      AS RACE,
       NULL                                      AS GENDER,
       MAILING_ADDRESS_1                         AS MAILING_ADDRESS_LINE_1,
       MAILING_ADDRESS_2                         AS MAILING_ADDRESS_LINE_2,
       NULL                                      AS MAILING_LINE_3,
       MAILING_CITY                              AS MAILING_CITY,
       MAILING_STATE                             AS MAILING_STATE,
       MAILING_ZIPCODE                           AS MAILING_ZIPCODE,
       NULL                                      AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.TEXAS_PROCESSED_DATA_2023_11_28 V
         INNER JOIN __dbt__cte__texas_counties C
                    ON V.COUNTY_CODE = C.COUNTY_ID
WHERE VUID NOT IN (
                   '2002582657',
                   '1031674244',
                   '1031674244',
                   '2002582657'
    )