WITH COUNTIES AS
         (SELECT C.NAME, S.CODE AS STATE_CODE
          FROM {{ ref("all_counties") }} C
                   INNER JOIN {{ ref("all_states") }} S ON C.STATEFP = S.FID)

SELECT *
FROM {{ ref("all_voters") }} V
WHERE NOT EXISTS (SELECT 1 FROM COUNTIES C WHERE C.STATE_CODE = V.STATE_CODE AND UPPER(C.NAME) = UPPER(V.COUNTY_CODE))