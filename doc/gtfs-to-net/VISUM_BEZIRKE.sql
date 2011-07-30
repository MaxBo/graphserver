set role ivm;
VACUUM ANALYSE "OBERBEZIRK";
VACUUM ANALYSE "BEZIRK";
VACUUM ANALYSE "FLAECHE";
VACUUM ANALYSE "FLAECHENELEMENT";
VACUUM ANALYSE "TEILFLAECHE";
VACUUM ANALYSE "TEILFLAECHENELEMENT";
VACUUM ANALYSE "KANTE";
VACUUM ANALYSE "PUNKT";
VACUUM ANALYSE "ZWISCHENPUNKT";

-- setze primary und foreign keys
ALTER TABLE "OBERBEZIRK" ADD CONSTRAINT "BEOBERBEZIRK_pkey" PRIMARY KEY ("NR");
ALTER TABLE "BEZIRK" ADD CONSTRAINT "BEZIRK_pkey" PRIMARY KEY ("NR");
-- setze für FBezirke und Oberbezirke ohne Fläche die FLAECHEID von 0 auf NULL
UPDATE "OBERBEZIRK" SET "FLAECHEID" = NULL WHERE "FLAECHEID" = 0;
UPDATE "BEZIRK" SET "FLAECHEID" = NULL WHERE "FLAECHEID" = 0;
UPDATE "BEZIRK" SET "OBEZNR" = NULL WHERE "OBEZNR" = 0;
ALTER TABLE "FLAECHE" ADD CONSTRAINT "FLAECHE_pkey" PRIMARY KEY ("ID");
ALTER TABLE "FLAECHENELEMENT" ADD CONSTRAINT "FLAECHENELEMENT_pkey" PRIMARY KEY ("FLAECHEID","TFLAECHEID" );
ALTER TABLE "TEILFLAECHE" ADD CONSTRAINT "TEILFLAECHE_pkey" PRIMARY KEY ("ID");
ALTER TABLE "TEILFLAECHENELEMENT" ADD CONSTRAINT "TEILFLAECHENELEMENT_pkey" PRIMARY KEY ("TFLAECHEID","INDEX" );
ALTER TABLE "KANTE" ADD CONSTRAINT "KANTE_pkey" PRIMARY KEY ("ID");
ALTER TABLE "PUNKT" ADD CONSTRAINT "PUNKT_pkey" PRIMARY KEY ("ID");
ALTER TABLE "ZWISCHENPUNKT" ADD CONSTRAINT "ZWISCHENPUNKT_pkey" PRIMARY KEY ("KANTEID","INDEX");
ALTER TABLE "BEZIRK_STRUKTURGR" ADD CONSTRAINT "BEZIRK_STRUKTURGR_pkey" PRIMARY KEY ("NR");
ALTER TABLE "BEZIRK_STRUKTURGR" DROP COLUMN
ALTER TABLE "BEZIRK_STRUKTURGR" ADD CONSTRAINT "BEZIRK_STRUKTURGR_BEZIRK_fkey" FOREIGN KEY ("NR")
REFERENCES "BEZIRK" ("NR") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "BEZIRK" ADD CONSTRAINT "BEZIRK_OBERBEZIRK_fkey" FOREIGN KEY ("OBEZNR")
REFERENCES "OBERBEZIRK" ("NR") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "BEZIRK" ADD CONSTRAINT "BEZIRK_FLAECHEID_fkey" FOREIGN KEY ("FLAECHEID")
REFERENCES "FLAECHE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "OBERBEZIRK" ADD CONSTRAINT "OBERBEZIRK_FLAECHEID_fkey" FOREIGN KEY ("FLAECHEID")
REFERENCES "FLAECHE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "FLAECHENELEMENT" ADD CONSTRAINT "FLAECHENELEMENT_FLAECHEID_fkey" FOREIGN KEY ("FLAECHEID")
REFERENCES "FLAECHE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "FLAECHENELEMENT" ADD CONSTRAINT "FLAECHENELEMENT_TEILFLAECHE_fkey" FOREIGN KEY ("TFLAECHEID")
REFERENCES "TEILFLAECHE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "TEILFLAECHENELEMENT" ADD CONSTRAINT "TEILFLAECHENELEMENT_TEILFLAECHE_fkey" FOREIGN KEY ("TFLAECHEID")
REFERENCES "TEILFLAECHE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "TEILFLAECHENELEMENT" ADD CONSTRAINT "TEILFLAECHENELEMENT_KANTE_fkey" FOREIGN KEY ("KANTEID")
REFERENCES "KANTE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE CASCADE;


ALTER TABLE "KANTE" ADD CONSTRAINT 
"KANTE_VONPUNKT_fkey" FOREIGN KEY ("VONPUNKTID")
REFERENCES "PUNKT" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "KANTE" ADD CONSTRAINT 
"KANTE_NACHPUNKT_fkey" FOREIGN KEY ("NACHPUNKTID")
REFERENCES "PUNKT" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "ZWISCHENPUNKT" ADD CONSTRAINT "ZWISCHENPUNKT_KANTE_fkey" FOREIGN KEY ("KANTEID")
REFERENCES "KANTE" ("ID") MATCH SIMPLE
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Lösche Kanten, die gleichen Start- und Zielpunkt haben und keine Zwischenpunkte haben
-- der Foreign key sorgt dafür, dass der Verweis auf diese Kanten auch aus der Teilflächenelement-Tabelle gelöscht wird
DELETE FROM "KANTE" k USING 
 (SELECT "ID"
  FROM "KANTE" k LEFT JOIN "ZWISCHENPUNKT" zp ON (k."ID" = zp."KANTEID")
  WHERE "VONPUNKTID"="NACHPUNKTID" AND zp."INDEX" IS NULL
 )einpunktkanten
WHERE k."ID" = einpunktkanten."ID"
;

-- baue Polylines für alle Kanten mit und ohne Zwischenpunkte

CREATE OR REPLACE VIEW kanten AS 
SELECT 
  kanten.id,
  st_setsrid(st_makeline(kanten.geom), 31467) AS geom
FROM 
(
 SELECT *
 FROM
 (
 -- erster Punkt
  SELECT 
    k."ID" AS id,
    0 AS index,
    st_makepoint(vp."XKOORD", vp."YKOORD") AS geom
  FROM 
    "PUNKT" vp,
    "KANTE" k
  WHERE 
    k."VONPUNKTID" = vp."ID"
    
  UNION
-- Zwischenpunkte, wenn vorhanden  
  SELECT
    zp."KANTEID" AS id, 
    zp."INDEX" AS index,
    st_makepoint(zp."XKOORD", zp."YKOORD") AS geom
  FROM 
    "ZWISCHENPUNKT" zp 

  UNION
-- Letzter Punkt
  SELECT
   k."ID" AS id,
    CASE
      WHEN max(zp."INDEX") IS NULL THEN 1 -- wenn keine Zwischenpunkte vorhanden
      ELSE max(zp."INDEX") + 1 --sonst letzter Index + 1
    END AS index,
    st_makepoint(np."XKOORD", np."YKOORD") AS geom
  FROM
    "PUNKT" np,
    "KANTE" k LEFT JOIN "ZWISCHENPUNKT" zp ON (k."ID" = zp."KANTEID")
  WHERE
    k."NACHPUNKTID" = np."ID"
  GROUP BY
    id,
    st_makepoint(np."XKOORD", np."YKOORD")
 ) AS k0
 ORDER BY
  k0.id,
  k0.index
) kanten
GROUP BY 
  kanten.id
;

CREATE OR REPLACE VIEW teilfl AS
SELECT 
  id,
  st_linemerge(st_collect(kgeom)) AS geom
FROM
 (
  SELECT
    tfle."TFLAECHEID" AS id,
    tfle."INDEX" AS index,
    CASE
      WHEN tfle."RICHTUNG" = 0 THEN k.geom -- Hinrichtung unverändert
      ELSE st_reverse(k.geom) --sonst Kante umdrehen
    END AS kgeom   
  FROM
    "TEILFLAECHENELEMENT" tfle,
    kanten k
  WHERE
    tfle."KANTEID" = k.id
  ORDER BY
    id,
    index
 )a
GROUP BY
  id
HAVING
  st_isclosed(st_linemerge(st_collect(kgeom)))
;

-- Spiele Infos zu Polygonen mit mehr als einer Teilfläche in temporäre Tabelle
SELECT
  b.*,
  teilfl.geom,
  st_area(st_makepolygon(teilfl.geom)) as area
INTO multipoly 
FROM
 (
  SELECT * 
  FROM 
    "FLAECHENELEMENT",
   (SELECT 
      "FLAECHEID" flid,
      count(*) cnt,
      sum("ENKLAVE") s
    FROM "FLAECHENELEMENT"
    GROUP BY "FLAECHEID"
   )a 
  WHERE
    cnt>1 
    AND 
    a.flid = "FLAECHEID"
 )b,
teilfl
WHERE b."TFLAECHEID" = teilfl.id
;

-- erzeuge View mit Polygonen 
CREATE OR REPLACE VIEW flaechenpoly AS

SELECT
  flid,
  st_union(geom) geom
FROM
 (SELECT 
    outerring.flid,
    st_makepolygon(outerring.geom,innerrings.geomcoll) geom
  FROM
    (SELECT
       "FLAECHEID" flid,
       st_accum(geom) geomcoll
     FROM
       multipoly mp
     WHERE
       "ENKLAVE" = 1
     GROUP BY
       "FLAECHEID"
    ) innerrings,
   (SELECT * FROM
     (SELECT
       "FLAECHEID" flid,
       first_value("TFLAECHEID") OVER(PARTITION BY "FLAECHEID" ORDER BY area DESC) AS tflid,
       first_value(geom) OVER(PARTITION BY "FLAECHEID" ORDER BY area DESC) AS geom
      FROM
        multipoly mp
      ) a
    GROUP BY
      a.flid,
      a.tflid,
      a.geom
   ) outerring
  WHERE outerring.flid=innerrings.flid

  UNION
  
  SELECT flid,geom FROM
   (SELECT
      rank() OVER (PARTITION BY flid ORDER BY area DESC) AS ranking,
      "FLAECHEID" flid,
      st_makepolygon(geom) geom,
      area
    FROM
      multipoly mp
    WHERE "ENKLAVE" = 0
   ) AS rr
  WHERE ranking > 1
 ) b
GROUP BY
  flid

UNION


SELECT
  flid,
  st_buildarea(teilfl.geom) geom
  
FROM
  teilfl, 
  "FLAECHENELEMENT" fle,
 (SELECT 
    "FLAECHEID" flid,
    count(*) cnt
  FROM "FLAECHENELEMENT"
  GROUP BY "FLAECHEID"
 )a 
WHERE
  cnt=1 
  AND 
  a.flid = "FLAECHEID"
  AND
  fle."TFLAECHEID" = teilfl.id
;

SELECT
  flid,
  st_makepolygon(teilfl.geom) geom,
  st_issimple(teilfl.geom)
  
FROM
  test_tf teilfl, 
  "FLAECHENELEMENT" fle,
 (SELECT 
    "FLAECHEID" flid,
    count(*) cnt
  FROM "FLAECHENELEMENT"
  GROUP BY "FLAECHEID"
 )a 
WHERE
  cnt=1 
  AND 
  a.flid = "FLAECHEID"
  AND
  fle."TFLAECHEID" = teilfl.id
  AND flid = 543343


SELECT * INTO flaechen FROM flaechenpoly;
ALTER TABLE flaechen ADD CONSTRAINT "flaechen_pkey" PRIMARY KEY (flid);
CREATE INDEX idx_flaechen_geom ON  flaechen USING GIST (geom);

SELECT *,st_astext(geom),st_npoints(geom) FROM flaechen WHERE st_geometrytype(geom) IS NULL
SELECT AddGeometryColumn('BEZIRK','geom',31467,'MULTIPOLYGON',2);
SELECT AddGeometryColumn('BEZIRK','xy',31467,'POINT',2);

UPDATE "BEZIRK" b SET geom = st_multi(fl.geom)
FROM flaechen fl
WHERE fl.flid = b."FLAECHEID"
;
UPDATE "BEZIRK" SET xy = ST_SetSRID(st_makepoint("XKOORD","YKOORD"),31467);
CREATE INDEX idx_bezirk_geom ON "BEZIRK" USING GIST (geom);
CREATE INDEX idx_bezirk_xy ON "BEZIRK" USING GIST (xy);

SELECT AddGeometryColumn('OBERBEZIRK','geom',31467,'MULTIPOLYGON',2);
SELECT AddGeometryColumn('OBERBEZIRK','xy',31467,'POINT',2);
UPDATE "OBERBEZIRK" ob SET geom = st_multi(fl.geom)
FROM flaechen fl
WHERE fl.flid = ob."FLAECHEID"
;
UPDATE "OBERBEZIRK" SET xy = ST_SetSRID(st_makepoint("XKOORD","YKOORD"),31467);
CREATE INDEX idx_oberbezirk_geom ON "OBERBEZIRK" USING GIST (geom);
CREATE INDEX idx_oberbezirk_xy ON "OBERBEZIRK" USING GIST (xy);

CREATE OR REPLACE VIEW bezirk AS
SELECT 
  b."NR" AS nr,
  b."NAME" AS name,
  b.geom,
  b.xy,
  sg."WERTSTRUKTURGROESSE(SG_AP)" AS sg_ap,
  sg."WERTSTRUKTURGROESSE(SG_APT)" AS sg_apt, 
  sg."WERTSTRUKTURGROESSE(SG_APT1)" AS sg_apt1, 
  sg."WERTSTRUKTURGROESSE(SG_EW)" AS sg_ew, 
  sg."WERTSTRUKTURGROESSE(SG_GSP)"  AS sg_gsp, 
  sg."WERTSTRUKTURGROESSE(SG_HSP)" AS sg_hsp, 
  sg."WERTSTRUKTURGROESSE(SG_KP)" AS sg_kp, 
  sg."WERTSTRUKTURGROESSE(SG_SSP)" AS sg_ssp, 
  sg."WERTSTRUKTURGROESSE(SG_VKFL)" AS sg_vkfl, 
  sg."WERTSTRUKTURGROESSE(SG_VKFT)" AS sg_vkft 
FROM
  "BEZIRK" b,
  "BEZIRK_STRUKTURGR" sg
WHERE b."NR" = sg."NR";

