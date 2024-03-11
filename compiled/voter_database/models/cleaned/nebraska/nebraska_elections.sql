

SELECT *
FROM (VALUES ('GN22', 'GENERAL', '11/8/2022'),
             ('PR22', 'PRIMARY', '5/10/2022'),
             ('GN20', 'GENERAL', '11/3/2020'),
             ('PR20', 'PRIMARY', '5/12/2020'),
             ('GN18', 'GENERAL', '11/6/2018'),
             ('PR18', 'PRIMARY', '5/15/2018'),
             ('GN16', 'GENERAL', '11/8/2016'),
             ('PR 16', 'PRIMARY', '5/10/2016'),
             ('GN14', 'GENERAL', '11/4/2014'),
             ('PR14', 'PRIMARY', '5/13/2014'),
             ('GN12', 'GENERAL', '11/6/2012'),
             ('PR12', 'PRIMARY', '5/15/2012'),
             ('GN10', 'GENERAL', '11/2/2010'),
             ('PR10', 'PRIMARY', '5/11/2010'),
             ('GN08', 'GENERAL', '11/4/2008'),
             ('PR08', 'PRIMARY', '5/13/2008'),
             ('GN06', 'GENERAL', '11/7/2006'),
             ('PR06', 'PRIMARY', '5/9/2006'))
         AS A(ELECTION_CODE, ELECTION_TYPE, ELECTION_DATE)