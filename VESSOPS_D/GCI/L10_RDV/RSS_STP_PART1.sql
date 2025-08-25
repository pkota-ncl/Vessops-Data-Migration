CREATE OR REPLACE PROCEDURE VESSOPS_D.L10_RDV.GCI_RSS_PRICING_PART1("DATE_FILTER" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
BEGIN


USE DATABASE VESSOPS_D;
USE SCHEMA L10_RDV;
CREATE OR REPLACE TEMP TABLE TEMP_U31 AS
SELECT DISTINCT 
    CruiseLineCode,
    ShipCode,
    CruiseLineName,
    SourceShipName,
    CruiseDuration,
    DisembarkDate AS DisembarkationDate,
    EmbarkationDate,
    EmbarkationPort,
    EmbarkationPortUN_LOCODE,
    DisembarkationPort,
    DisembarkationPortUN_LOCODE,
    ItineraryPortContent,
    FilePath
FROM VESSOPS_D.L10_RDV.GCI_Raw_Pricing_RG_Pass
WHERE TRY_TO_DATE(SUBSTRING(FilePath, 42, 10), 'YYYY-MM-DD') IS NOT NULL
  AND TRY_TO_DATE(SUBSTRING(FilePath, 42, 10), 'YYYY-MM-DD') > :DATE_FILTER
  AND DAYOFWEEK(TRY_TO_DATE(SUBSTRING(FilePath, 42, 10), 'YYYY-MM-DD')) = 1
  AND UPPER(TRIM(RateType)) = 'DOUBLE'
  AND UPPER(TRIM(CruiseLineName)) IN (
      'REGENT SEVEN SEAS', 'SILVERSEA CRUISES', 'SEABOURN CRUISES',
      'CRYSTAL CRUISES', 'RITZCARLTON', 'PONANT CRUISE LINE',
      'HAPAG-LLOYD CRUISES', 'VIKING OCEAN CRUISES', 'EXPLORAJOURNEY'
  );

CREATE OR REPLACE TEMP TABLE TEMP_U32 AS
SELECT 
    CruiseLineCode,
    ShipCode,
    CruiseLineName,
    SourceShipName,
    CruiseDuration,
    DisembarkationDate,
    EmbarkationDate,
    EmbarkationPort,
    EmbarkationPortUN_LOCODE,
    DisembarkationPort,
    DisembarkationPortUN_LOCODE,
    REPLACE(ItineraryPortContent, '@$#', '+') AS ItineraryPortContent,
    FilePath
FROM TEMP_U31;

CREATE OR REPLACE TEMP TABLE TEMP_U33 AS
SELECT t2.*, BK.value AS ItineraryPortSplit
FROM TEMP_U32 t2,
LATERAL FLATTEN(input => SPLIT(t2.ItineraryPortContent, '+')) AS BK;

CREATE OR REPLACE TEMP TABLE TEMP_U34 AS
SELECT *, REPLACE(ItineraryPortSplit, '@$', '+') AS ItinPort_ItinDate
FROM TEMP_U33;

CREATE OR REPLACE TEMP TABLE TEMP_U35 AS
SELECT *, REPLACE(ItinPort_ItinDate, '.', '>') AS stk4
FROM TEMP_U34;

CREATE OR REPLACE TEMP TABLE TEMP_U36 AS
SELECT 
    CruiseLineCode,
    ShipCode,
    CruiseLineName,
    SourceShipName,
    CruiseDuration,
    DisembarkationDate,
    EmbarkationDate,
    EmbarkationPort,
    EmbarkationPortUN_LOCODE,
    DisembarkationPort,
    DisembarkationPortUN_LOCODE,
    ItineraryPortContent,
    FilePath,
    SPLIT_PART(stk4, '+', 1) AS Port_City_Nam,
    SPLIT_PART(stk4, '+', 2) AS Itinerary_Date
FROM TEMP_U35
ORDER BY Itinerary_Date;

CREATE OR REPLACE TEMP TABLE TEMP_U37 AS
SELECT 
    CruiseLineCode,
    ShipCode,
    CruiseLineName,
    SourceShipName,
    CruiseDuration,
    DisembarkationDate,
    EmbarkationDate,
    EmbarkationPort,
    EmbarkationPortUN_LOCODE,
    DisembarkationPort,
    DisembarkationPortUN_LOCODE,
    ItineraryPortContent,
    FilePath,
    REPLACE(Port_City_Nam, '>', '.') AS Port_City_Nam,
    Itinerary_Date
FROM TEMP_U36;

DELETE FROM TEMP_U37
WHERE TRY_TO_DATE(Itinerary_Date, 'MM-DD-YYYY') 
      NOT BETWEEN CAST(EmbarkationDate AS DATE) 
      AND CAST(DisembarkationDate AS DATE);

CREATE OR REPLACE TEMP TABLE TEMP_U38 AS
SELECT 
    mbs.MP_CruiseLineCode AS CruiseLineCode,
    mbs.MP_ShipCode AS ShipCode,
    mbs.MP_CruiseLineName AS CruiseLineName,
    mbs.MP_SourceShipName AS SourceShipName,
    ps.CruiseDuration,
    ps.DisembarkationDate,
    ps.EmbarkationDate,
    nd.Tb_Port_Loc_Name AS EmbarkationPort,
    nd.Tb_Unv_Port_Code AS EmbarkationPortUN_LOCODE,
    nd1.Tb_Port_Loc_Name AS DisembarkationPort,
    nd1.Tb_Unv_Port_Code AS DisembarkationPortUN_LOCODE,
    ps.ItineraryPortContent,
    nd3.Tb_Port_Loc_Name AS Port_City_Nam,
    nd3.Tb_Unv_Port_Code AS PortUN_LOCODE,
    nd3.Tb_Country_Name AS Port_Country_Nam,
    ps.FilePath,
    ps.Itinerary_Date
FROM TEMP_U37 ps
LEFT JOIN VESSOPS_D.L10_RDV.GCI_Raw_Ref_Merged_Port_data nd
    ON UPPER(TRIM(ps.EmbarkationPortUN_LOCODE)) = UPPER(TRIM(nd.Dtl_PortUN_Locode))
    AND UPPER(REGEXP_REPLACE(TRIM(ps.EmbarkationPort), '\\s+', ' ')) = UPPER(REGEXP_REPLACE(TRIM(nd.Dtl_Port_City_Nam), '\\s+', ' '))
LEFT JOIN VESSOPS_D.L10_RDV.GCI_Raw_Ref_Merged_Port_data nd1
    ON UPPER(TRIM(ps.DisembarkationPortUN_LOCODE)) = UPPER(TRIM(nd1.Dtl_PortUN_Locode))
    AND UPPER(REGEXP_REPLACE(TRIM(ps.DisembarkationPort), '\\s+', ' ')) = UPPER(REGEXP_REPLACE(TRIM(nd1.Dtl_Port_City_Nam), '\\s+', ' '))
LEFT JOIN VESSOPS_D.L10_RDV.GCI_Raw_Ref_Merged_Ship_data mbs
    ON UPPER(TRIM(ps.CruiseLineName)) = UPPER(TRIM(mbs.CruiseLineName))
    AND UPPER(TRIM(ps.CruiseLineCode)) = UPPER(TRIM(mbs.CruiseLineCode))
    AND UPPER(TRIM(ps.SourceShipName)) = UPPER(TRIM(mbs.SourceShipName))
    AND UPPER(TRIM(ps.ShipCode)) = UPPER(TRIM(mbs.ShipCode))
LEFT JOIN VESSOPS_D.L10_RDV.GCI_Raw_Ref_Merged_Port_data nd3
    ON UPPER(TRIM(ps.Port_City_Nam)) = UPPER(TRIM(nd3.dtl_port_city_nam));

INSERT INTO VESSOPS_D.L10_RDV.GCI_RSS_INTER_PRICING_VOY_DAY_LEVEL_DATA
WITH src AS (
  SELECT
    u.*,
    CONCAT(
      CruiseLineCode, '-', ShipCode, '-', CAST(EmbarkationDate AS DATE), '-',
      EmbarkationPortUN_LOCODE, '-', DisembarkationPortUN_LOCODE, '-', CruiseDuration
    ) AS NCLH_UNI_VOYAGE_CODE
  FROM TEMP_U38 u
)
SELECT
  s.*,
  CURRENT_TIMESTAMP AS LDTS,
  CURRENT_USER AS LAST_MODIFIED_BY,
  'RG_PRICNG_DATA,REF_PORT_DATA' AS RCSR,
  TO_VARCHAR(MD5(CONCAT_WS('|',
    COALESCE(s.CruiseLineCode, ''),
    COALESCE(s.ShipCode, ''),
    COALESCE(TO_VARCHAR(s.EmbarkationDate, 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
    COALESCE(s.EmbarkationPortUN_LOCODE, ''),
    COALESCE(s.DisembarkationPortUN_LOCODE, ''),
    COALESCE(TO_VARCHAR(s.CruiseDuration), ''),
    COALESCE(s.NCLH_UNI_VOYAGE_CODE, '')
  ))) AS HASH_DIFF
FROM src s;

DELETE FROM VESSOPS_D.L10_RDV.GCI_RSS_INTER_PRICING_VOY_DAY_LEVEL_DATA
WHERE CruiseLineCode IS NULL OR ShipCode IS NULL OR CruiseLineName IS NULL 
  OR SourceShipName IS NULL OR CruiseDuration IS NULL 
  OR DisembarkationDate IS NULL OR EmbarkationDate IS NULL 
  OR EmbarkationPort IS NULL OR EmbarkationPortUN_LOCODE IS NULL 
  OR DisembarkationPort IS NULL OR DisembarkationPortUN_LOCODE IS NULL 
  OR ItineraryPortContent IS NULL OR Port_City_Nam IS NULL 
  OR PortUN_LOCODE IS NULL OR Port_Country_Nam IS NULL 
  OR FilePath IS NULL OR Itinerary_Date IS NULL 
  OR NCLH_UNI_VOYAGE_CODE IS NULL;

CREATE OR REPLACE TEMP TABLE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA AS
SELECT *
FROM (
  SELECT
    d.*,
    RANK() OVER (PARTITION BY d.NCLH_UNI_VOYAGE_CODE ORDER BY d.FilePath DESC) AS ranker1
  FROM VESSOPS_D.L10_RDV.GCI_RSS_INTER_PRICING_VOY_DAY_LEVEL_DATA d
)
WHERE ranker1 = 1;

ALTER TABLE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA DROP COLUMN HASH_DIFF;

ALTER TABLE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA
ADD COLUMN 
  VESSEL_IMO VARCHAR(50),
  NCLH_SHIP_CODE VARCHAR(5),
  SAIL_ID VARCHAR(50),
  MD5_VOYAGE_HASH_KEY VARCHAR,
  HASH_DIFF VARCHAR;

UPDATE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA AS a
SET VESSEL_IMO = b.VESSEL_IMO_NO, NCLH_SHIP_CODE = b.SHIP_CODE_3L
FROM VESSOPS_D.L00_STG.GCI_RAW_REF_SHIP_DATA AS b
WHERE CONCAT(a.CRUISELINECODE, a.SHIPCODE) = b."5L_SHIP_CODE";

UPDATE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA
SET SAIL_ID = CONCAT(NCLH_SHIP_CODE, TO_CHAR(EMBARKATIONDATE, 'YYMMDD'));

UPDATE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA
SET MD5_VOYAGE_HASH_KEY = MD5(SAIL_ID);

UPDATE TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA
SET HASH_DIFF = TO_VARCHAR(MD5(CONCAT_WS('|',
  COALESCE(CruiseLineCode, ''),
  COALESCE(ShipCode, ''),
  COALESCE(TO_VARCHAR(EmbarkationDate, 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
  COALESCE(EmbarkationPortUN_LOCODE, ''),
  COALESCE(DisembarkationPortUN_LOCODE, ''),
  COALESCE(TO_VARCHAR(CruiseDuration), ''),
  COALESCE(NCLH_UNI_VOYAGE_CODE, ''),
  COALESCE(TO_VARCHAR(VESSEL_IMO), ''),
  COALESCE(NCLH_SHIP_CODE, ''),
  COALESCE(SAIL_ID, '')
)));

TRUNCATE TABLE VESSOPS_D.L10_RDV.GCI_RSS_INTER_REF_PRICING_VOY_DAY_LEVEL_DATA;

INSERT INTO VESSOPS_D.L10_RDV.GCI_RSS_INTER_REF_PRICING_VOY_DAY_LEVEL_DATA
SELECT * FROM TEMP_GCI_INTER_REF_PRICING_RSS_VOY_DAY_LEVEL_DATA;

  RETURN 'GCI_RSS_Part1  STP Successfully Completed';
END;
$$;



