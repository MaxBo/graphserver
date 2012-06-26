set role max;
-- alle Strecken mit Zwischenpunkten in "Streckenpolygonen" sowie die Hin-Richtung aller Strecken ohne Zwischenpunkt 
CREATE OR REPLACE VIEW strecken_hin AS 
SELECT
  s2."VONKNOTNR",
  s2."NACHKNOTNR"
FROM 
( 
  SELECT 
    s1."VONKNOTNR",
    s1."NACHKNOTNR",
    row_number() OVER (ORDER BY s1."NR", s1.idx) AS rn
  FROM 
  ( 
    SELECT 
      s."NR",
      s."VONKNOTNR",
      s."NACHKNOTNR",
      min(sp."INDEX") AS idx
    FROM 
      "STRECKE" s LEFT JOIN "STRECKENPOLY" sp ON (s."VONKNOTNR" = sp."VONKNOTNR" AND s."NACHKNOTNR" = sp."NACHKNOTNR")
    GROUP BY
      s."NR",
      s."VONKNOTNR",
      s."NACHKNOTNR"
  ) s1
) s2
WHERE 
  (s2.rn % 2::bigint) = 1
;

-- baue Polylines für alle Strecken mit und ohne Streckenpolygone

CREATE OR REPLACE VIEW strpoly AS 
SELECT 
  poly."VONKNOTNR",
  poly."NACHKNOTNR",
  st_setsrid(st_makeline(poly.geom), 31467) AS geom
FROM 
(
-- erster Punkt
  SELECT 
    sh."VONKNOTNR",
    sh."NACHKNOTNR",
    0 AS index,
    st_makepoint(vk."XKOORD", vk."YKOORD") AS geom
  FROM 
    "KNOTEN" vk,
    strecken_hin sh LEFT JOIN "STRECKENPOLY" sp ON (sh."VONKNOTNR" = sp."VONKNOTNR" AND sh."NACHKNOTNR" = sp."NACHKNOTNR")
  WHERE 
    sh."VONKNOTNR" = vk."NR"
    
  UNION
-- Zwischenpunkte, wenn vorhanden  
  SELECT
    sh."VONKNOTNR",
    sh."NACHKNOTNR",
    sp."INDEX",
    st_makepoint(sp."XKOORD", sp."YKOORD") AS geom
  FROM 
    strecken_hin sh,
    "STRECKENPOLY" sp 
  WHERE
    sh."VONKNOTNR" = sp."VONKNOTNR" AND sh."NACHKNOTNR" = sp."NACHKNOTNR"
  ORDER BY
    sh."VONKNOTNR",
    sh."NACHKNOTNR",
    sp."INDEX"

  UNION
-- Letzter Punkt
  SELECT
    sh."VONKNOTNR",
    sh."NACHKNOTNR", 
    CASE
      WHEN max(sp."INDEX") IS NULL THEN 1 -- wenn keine Zwischenpunkte vorhanden
      ELSE max(sp."INDEX") + 1 --sonst letzter Index + 1
    END AS index,
    st_makepoint(nk."XKOORD", nk."YKOORD") AS geom
  FROM
    "KNOTEN" nk,
    strecken_hin sh LEFT JOIN ("STRECKENPOLY" sp ON sh."VONKNOTNR" = sp."VONKNOTNR" AND sh."NACHKNOTNR" = sp."NACHKNOTNR")
  WHERE
    sh."NACHKNOTNR" = nk."NR"
  GROUP BY
    sh."VONKNOTNR",
    sh."NACHKNOTNR",
    st_makepoint(nk."XKOORD", nk."YKOORD")
) AS poly
GROUP BY 
  poly."VONKNOTNR",
  poly."NACHKNOTNR"
;


CREATE OR REPLACE VIEW linienrouten AS 
SELECT
  lre2.lrname, 
  st_collect(sp.geom) AS geom
FROM 
( -- 
  SELECT 
    lre1.lrname,
    lre1.idx,
    lre1.knr AS "VONKNOTNR",
    lag(lre1.knr) OVER (ORDER BY lre1.lr, lre1.idx) AS "NACHKNOTNR" -- letzte Zeile
  FROM
  (
    SELECT
      lre."LINNAME" || '_' || lre."LINROUTENAME" || rc."Name" AS lrname, --Bilde lrname aus Linienname, Linienroutennummer und Richtungscode
      lre."INDEX" AS idx,
      lre."KNOTNR" AS knr
    FROM
      "LINIENROUTENELEMENT" lre,
      "Richtungscode" rc
    WHERE
      lre."RICHTUNGCODE"::character(1) = rc."Code"
  ) AS lre1
) AS lre2,
(
  SELECT
    sph."VONKNOTNR",
    sph."NACHKNOTNR",
    sph.geom
  FROM strpoly sph
  
  UNION 
  
  SELECT
    spr."NACHKNOTNR" AS "VONKNOTNR",
    spr."VONKNOTNR" AS "NACHKNOTNR",
    st_reverse(spr.geom) AS geom
  FROM strpoly spr
) sp
WHERE
  lre2."VONKNOTNR" = sp."VONKNOTNR" AND lre2."NACHKNOTNR" = sp."NACHKNOTNR"
GROUP BY
  lre2.lrname
;

