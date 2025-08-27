--select top 10 * from vessops_d.l00_stg.gci_raw_ref_ship_data

CREATE OR REPLACE TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA (
    "5L_SHIP_CODE"             VARCHAR(255)      COMMENT 'External/5L ship code if provided',
    CRUISE_LINE_CODE           VARCHAR(255)      COMMENT 'Cruise line code',
    SHIP_ID                    NUMBER(38,0)      COMMENT 'Internal ship identifier',
    SHIP_CODE                  VARCHAR(255)      COMMENT 'Ship code',
    NCLH_RES_SHIP_CD           VARCHAR(255)      COMMENT 'Reservation system ship code (NCLH)',
    NCLH_DW_SHIP_CD            VARCHAR(255)      COMMENT 'Data warehouse ship code (NCLH)',
    NCLH_MXP_SHIP_CD           VARCHAR(255)      COMMENT 'MXP ship code (NCLH)',
    BRAND_CATEGORY             VARCHAR(255)      COMMENT 'Brand category (e.g., Contemporary/Premium/Luxury)',
    BRAND_FOLIO                VARCHAR(255)      COMMENT 'Brand folio/ledger grouping',
    BRAND_NAME                 VARCHAR(255)      COMMENT 'Commercial brand name',
    SHIP_NAME                  VARCHAR(255)      COMMENT 'Marketing/official ship name',
    "Segment"                  VARCHAR(255)      COMMENT 'Market segment',
    "Sub-Segment"              VARCHAR(255)      COMMENT 'Market sub-segment',
    "Market"                   VARCHAR(255)      COMMENT 'Sales/geo market',
    ACTIVE_SHIP                NUMBER(38,0)      COMMENT 'Active ship flag (1 active, 0 inactive)',
    "Present_in_Capacity_2020" FLOAT             COMMENT 'Flag/metric indicating presence in 2020 capacity set',
    SHIP_REGISTRY              VARCHAR(255)      COMMENT 'Port/country of registry',
    SHIP_CLASS_CODE            VARCHAR(255)      COMMENT 'Ship class code',
    ALB                        NUMBER(38,0)      COMMENT 'Available lower berths (ALB)',
    CREW_COUNT                 NUMBER(38,0)      COMMENT 'Crew complement',
    STATUS                     FLOAT             COMMENT 'Operational status code/value',
    STATUS_DATE                VARCHAR(255)      COMMENT 'Status as-of date (source native format)',
    LAST_ACTIVITY_TYPE         VARCHAR(255)      COMMENT 'Last activity type/event from source',
    GROSS                      FLOAT             COMMENT 'Gross tonnage',
    VESSEL_IMO_NO              FLOAT            COMMENT 'IMO number (key); consider NUMBER(9,0) or VARCHAR(7) to avoid float rounding',
    VESSEL_MMSI_NO             FLOAT             COMMENT 'MMSI number',
    YEAR_BUILT                 FLOAT             COMMENT 'Year built',
    YEAR_LAST_REFURB           FLOAT             COMMENT 'Year of last refurbishment',
    STUDIO_CABINS_ALBS         FLOAT             COMMENT 'ALBs in studio cabins',
    INSIDE_CABINS_ALBS         FLOAT             COMMENT 'ALBs in inside cabins',
    OUTSIDE_CABINS_ALBS        FLOAT             COMMENT 'ALBs in oceanview cabins',
    BALCONY_CABINS_ALBS        NUMBER(38,0)      COMMENT 'ALBs in balcony cabins',
    SUITE_CABINS_ALBS          NUMBER(38,0)      COMMENT 'ALBs in suites',
    TOTAL_ALBS                 NUMBER(38,0)      COMMENT 'Total ALBs',
    MAX_GUEST_CAPACITY         NUMBER(38,0)      COMMENT 'Maximum guest capacity',
    LENGTH                     NUMBER(38,0)      COMMENT 'Overall length',
    BREADTH                    NUMBER(38,0)      COMMENT 'Beam/breadth',
    DRAFT                      FLOAT             COMMENT 'Draft',
    HEIGHT                     FLOAT             COMMENT 'Air draft/height',
    SHIP_YARD                  VARCHAR(255)      COMMENT 'Shipyard/builder',
    PROPULSION                 VARCHAR(255)      COMMENT 'Propulsion type/notes',
    CNT_STUDIO_CABINS_CAP      NUMBER(5,0)       COMMENT 'Count of studio cabins',
    CNT_INSIDE_CABINS_CAP      NUMBER(5,0)       COMMENT 'Count of inside cabins',
    CNT_OUTSIDE_CABINS_CAP     NUMBER(5,0)       COMMENT 'Count of outside/oceanview cabins',
    CNT_BALCONY_CABINS_CAP     NUMBER(5,0)       COMMENT 'Count of balcony cabins',
    CNT_SUITE_CABINS_CAP       NUMBER(5,0)       COMMENT 'Count of suites',
    TOTAL_CABINS               NUMBER(38,0)      COMMENT 'Total cabins count',
    IN_SERVICE_DATE            TIMESTAMP_NTZ(9)  COMMENT 'Date entered service',
    OUT_OF_SERVICE_DATE        TIMESTAMP_NTZ(9)  COMMENT 'Date left service',
    "Comments"                 VARCHAR(255)      COMMENT 'Free-form comments from source',
    "Original_Delivery_Date"   TIMESTAMP_NTZ(9)  COMMENT 'Original delivery date',
    "Ops_Status"               VARCHAR(255)      COMMENT 'Operational status label',
    "Source_Link"              VARCHAR(255)      COMMENT 'Source URL/reference',
    SHIP_CODE_3L                VARCHAR(3),

    -- /* Additional metadata columns */
    -- MD5_SHIP_HASH_KEY        VARCHAR           COMMENT 'Deterministic MD5 hash of voyage/business key columns',
    -- HASH_DIFF                  VARCHAR           COMMENT 'Hash diff for Type 1/CDC change detection',
    -- LDTS                       TIMESTAMP_NTZ(9)  DEFAULT CURRENT_TIMESTAMP() COMMENT 'Load date timestamp (system)',
    -- RCSR                       VARCHAR(200)      COMMENT 'Record source (system/file/process origin)',
    -- LAST_MODIFIED_BY           VARCHAR(500)      DEFAULT CURRENT_USER()      COMMENT 'User/principal that last modified the row',

    CONSTRAINT PK_GCI_RAW_REF_SHIP_DATA PRIMARY KEY (VESSEL_IMO_NO)
)
COMMENT = 'Raw reference ship/vessel attributes from GCI source with metadata for lineage and change detection';


--select * from  VESSOPS_D.L10_RDV.GCI_RAW_REF_SHIP_DATA

insert into VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_SHIP_DATA

alter table VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA
add column  MD5_SHIP_HASH_KEY VARCHAR,
    HASH_DIFF VARCHAR,
    LDTS TIMESTAMP_NTZ(9),
    RCSR VARCHAR(200),
    LAST_MODIFIED_BY VARCHAR;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_SHIP_DATA
-- add column SHIP_CODE_3L VARCHAR(3);
    
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA
SET MD5_SHIP_HASH_KEY = MD5(VESSEL_IMO_NO);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA
SET 
    LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA
SET HASH_DIFF = UPPER(
    HEX_ENCODE(
      MD5(
        TO_JSON(ARRAY_CONSTRUCT(
          /* VARCHARs (trim/upper for case-insensitive normalization) */
          UPPER(TRIM(COALESCE("5L_SHIP_CODE", ''))),
          UPPER(TRIM(COALESCE(CRUISE_LINE_CODE, ''))),
          UPPER(TRIM(COALESCE(SHIP_CODE, ''))),
          UPPER(TRIM(COALESCE(NCLH_RES_SHIP_CD, ''))),
          UPPER(TRIM(COALESCE(NCLH_DW_SHIP_CD, ''))),
          UPPER(TRIM(COALESCE(NCLH_MXP_SHIP_CD, ''))),
          UPPER(TRIM(COALESCE(BRAND_CATEGORY, ''))),
          UPPER(TRIM(COALESCE(BRAND_FOLIO, ''))),
          UPPER(TRIM(COALESCE(BRAND_NAME, ''))),
          UPPER(TRIM(COALESCE(SHIP_NAME, ''))),
          UPPER(TRIM(COALESCE("Segment", ''))),
          UPPER(TRIM(COALESCE("Sub-Segment", ''))),
          UPPER(TRIM(COALESCE("Market", ''))),
          UPPER(TRIM(COALESCE(SHIP_REGISTRY, ''))),
          UPPER(TRIM(COALESCE(SHIP_CLASS_CODE, ''))),
          UPPER(TRIM(COALESCE(STATUS_DATE, ''))),
          UPPER(TRIM(COALESCE(LAST_ACTIVITY_TYPE, ''))),
          UPPER(TRIM(COALESCE(SHIP_YARD, ''))),
          UPPER(TRIM(COALESCE(PROPULSION, ''))),
          UPPER(TRIM(COALESCE("Comments", ''))),
          UPPER(TRIM(COALESCE("Ops_Status", ''))),
          UPPER(TRIM(COALESCE("Source_Link", ''))),

          /* NUMBER/integer-like columns */
          COALESCE(TO_VARCHAR(SHIP_ID), ''),
          COALESCE(TO_VARCHAR(ACTIVE_SHIP), ''),
          COALESCE(TO_VARCHAR(ALB), ''),
          COALESCE(TO_VARCHAR(CREW_COUNT), ''),
          COALESCE(TO_VARCHAR(BALCONY_CABINS_ALBS), ''),
          COALESCE(TO_VARCHAR(SUITE_CABINS_ALBS), ''),
          COALESCE(TO_VARCHAR(TOTAL_ALBS), ''),
          COALESCE(TO_VARCHAR(MAX_GUEST_CAPACITY), ''),
          COALESCE(TO_VARCHAR(LENGTH), ''),
          COALESCE(TO_VARCHAR(BREADTH), ''),
          COALESCE(TO_VARCHAR(CNT_STUDIO_CABINS_CAP), ''),
          COALESCE(TO_VARCHAR(CNT_INSIDE_CABINS_CAP), ''),
          COALESCE(TO_VARCHAR(CNT_OUTSIDE_CABINS_CAP), ''),
          COALESCE(TO_VARCHAR(CNT_BALCONY_CABINS_CAP), ''),
          COALESCE(TO_VARCHAR(CNT_SUITE_CABINS_CAP), ''),
          COALESCE(TO_VARCHAR(TOTAL_CABINS), ''),

          /* FLOAT/real-valued columns (rounded for determinism) */
          COALESCE(TO_VARCHAR(ROUND("Present_in_Capacity_2020", 6)), ''),
          COALESCE(TO_VARCHAR(ROUND(STATUS, 6)), ''),
          COALESCE(TO_VARCHAR(ROUND(GROSS, 6)), ''),
          COALESCE(TO_VARCHAR(DRAFT), ''),
          COALESCE(TO_VARCHAR(HEIGHT), ''),
          COALESCE(TO_VARCHAR(STUDIO_CABINS_ALBS), ''),
          COALESCE(TO_VARCHAR(INSIDE_CABINS_ALBS), ''),
          COALESCE(TO_VARCHAR(OUTSIDE_CABINS_ALBS), ''),

          /* Identifiers (keep raw, no rounding) */
          COALESCE(TO_VARCHAR(VESSEL_IMO_NO), ''),
          COALESCE(TO_VARCHAR(VESSEL_MMSI_NO), ''),
          COALESCE(TO_VARCHAR(YEAR_BUILT), ''),
          COALESCE(TO_VARCHAR(YEAR_LAST_REFURB), ''),

          /* Timestamps (explicit ISO-8601 text for determinism) */
          COALESCE(TO_VARCHAR(IN_SERVICE_DATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
          COALESCE(TO_VARCHAR(OUT_OF_SERVICE_DATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
          COALESCE(TO_VARCHAR("Original_Delivery_Date", 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), '')
        )))
      )
    )
;


CREATE OR REPLACE TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA (
    PORT_ID                 VARCHAR(6)   COMMENT 'Source port identifier',
    PORT_LOC_NAME           VARCHAR(90)  COMMENT 'Port/location display name',
    PORT_COUNTRY_CODE       VARCHAR(3)   COMMENT 'Country code (3-char, source-defined)',
    PORT_LOC_CODE           VARCHAR(4)   COMMENT 'Port/location code (4-char, source-defined)',
    STATUS                  VARCHAR(4)   COMMENT 'Operational status code',
    STATUS_DATE             VARCHAR(31)  COMMENT 'Status as-of date (source native format)',
    LAST_ACTIVITY_TYPE      VARCHAR(4)   COMMENT 'Last recorded activity type',
    PORT_IND                VARCHAR(4)   COMMENT 'Port indicator flag/code',
    UTC                     VARCHAR(4)   COMMENT 'UTC offset text (e.g., +01, -05)',
    IATA_AIRPORT_CODE       VARCHAR(4)   COMMENT 'Associated IATA airport code',
    PORT_PIER_CAPACITY      VARCHAR(4)   COMMENT 'Pier capacity (source text/units)',
    CONTINENT               VARCHAR(13)  COMMENT 'Continent name',
    COUNTRY_NAME            VARCHAR(44)  COMMENT 'Country name',

    PORT_LAT                NUMBER(10,2) COMMENT 'Latitude (decimal degrees)',
    PORT_LONG               NUMBER(10,2) COMMENT 'Longitude (decimal degrees)',
    PORT_COORDINATES        VARCHAR(14)  COMMENT 'Coordinates as text (lat,long)',

    SCENIC_IND              VARCHAR(1)   COMMENT 'Scenic indicator (Y/N or code)',
    UNV_PORT_CODE           VARCHAR(6)   NOT NULL COMMENT 'Universal/internal port code (primary key)',
    NCLH_RM_TRADE1          VARCHAR(13)  COMMENT 'Trade/market mapping level 1',
    NCLH_RM_TRADE2          VARCHAR(23)  COMMENT 'Trade/market mapping level 2',
    NCLH_RM_TRADE3          VARCHAR(31)  COMMENT 'Trade/market mapping level 3',
    NCLH_RM_TRADE4          VARCHAR(12)  COMMENT 'Trade/market mapping level 4',
    RMS_EMBARK_REGION_4     VARCHAR(12)  COMMENT 'Legacy embark region (level 4)',
    COMMENTS                VARCHAR(86)  COMMENT 'Free-form comments/notes',
    REGION_ID               VARCHAR(4)   COMMENT 'Region identifier code',
    PORT_OWNERSHIP          VARCHAR(9)   COMMENT 'Ownership type/label',
    CORP_BERTH_CAPACITY     VARCHAR(4)   COMMENT 'Corporate berth capacity (text)',
    TENDER                  VARCHAR(4)   COMMENT 'Tender required flag/code',
    SEAWARE_PORT_CODE       VARCHAR(4)   COMMENT 'Seaware system port code',
    NVS_PORT_CODE           VARCHAR(4)   COMMENT 'NVS system port code',
    MXP_PORT_CODE           VARCHAR(4)   COMMENT 'MXP system port code',
    NEW_EMBARK_REGION_1     VARCHAR(14)  COMMENT 'New embark region mapping level 1',
    NEW_EMBARK_REGION_2     VARCHAR(20)  COMMENT 'New embark region mapping level 2',
    NEW_EMBARK_REGION_3     VARCHAR(60)  COMMENT 'New embark region mapping level 3',
    UNV_PORT_CODE_FLAG      VARCHAR(1)   COMMENT 'Flag indicating UNV port code status/validity',

    CONSTRAINT PK_GCI_RAW_REF_PORT_DATA PRIMARY KEY (UNV_PORT_CODE)
)
COMMENT = 'Port reference attributes, codes, geolocation, and trade/region mappings (GCI source).';


insert into VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_PORT_DATA;

alter table VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA
add column MD5_PORT_HASH_KEY VARCHAR,
    HASH_DIFF VARCHAR,
    LDTS TIMESTAMP_NTZ(9),
    RCSR VARCHAR(200),
    LAST_MODIFIED_BY VARCHAR;

    
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA
SET MD5_PORT_HASH_KEY = MD5(UNV_PORT_CODE);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA
SET 
    LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

/* Build HASH_DIFF over all non-metadata columns (in table order) */
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_PORT_DATA
SET HASH_DIFF = TO_VARCHAR(
  MD5(
    CONCAT_WS(
      '|',
      COALESCE(PORT_ID, ''),
      COALESCE(PORT_LOC_NAME, ''),
      COALESCE(PORT_COUNTRY_CODE, ''),
      COALESCE(PORT_LOC_CODE, ''),
      COALESCE(STATUS, ''),
      COALESCE(STATUS_DATE, ''),
      COALESCE(LAST_ACTIVITY_TYPE, ''),
      COALESCE(PORT_IND, ''),
      COALESCE(UTC, ''),
      COALESCE(IATA_AIRPORT_CODE, ''),
      COALESCE(PORT_PIER_CAPACITY, ''),
      COALESCE(CONTINENT, ''),
      COALESCE(COUNTRY_NAME, ''),
      COALESCE(TO_VARCHAR(PORT_LAT), ''),
      COALESCE(TO_VARCHAR(PORT_LONG), ''),
      COALESCE(PORT_COORDINATES, ''),
      COALESCE(SCENIC_IND, ''),
      COALESCE(UNV_PORT_CODE, ''),
      COALESCE(NCLH_RM_TRADE1, ''),
      COALESCE(NCLH_RM_TRADE2, ''),
      COALESCE(NCLH_RM_TRADE3, ''),
      COALESCE(NCLH_RM_TRADE4, ''),
      COALESCE(RMS_EMBARK_REGION_4, ''),
      COALESCE(COMMENTS, ''),
      COALESCE(REGION_ID, ''),
      COALESCE(PORT_OWNERSHIP, ''),
      COALESCE(CORP_BERTH_CAPACITY, ''),
      COALESCE(TENDER, ''),
      COALESCE(SEAWARE_PORT_CODE, ''),
      COALESCE(NVS_PORT_CODE, ''),
      COALESCE(MXP_PORT_CODE, ''),
      COALESCE(NEW_EMBARK_REGION_1, ''),
      COALESCE(NEW_EMBARK_REGION_2, ''),
      COALESCE(NEW_EMBARK_REGION_3, ''),
      COALESCE(UNV_PORT_CODE_FLAG, '')
    )
  )
);

--select top 10 * from VESSOPS_D.L10_RDV.GCI_RAW_REF_PORT_DATA

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS


CREATE OR REPLACE TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS (
    RATEINQUIRYDATE                TIMESTAMP_NTZ(9)  NOT NULL COMMENT 'Shop/inquiry timestamp from source (part of PK)',
    SOURCEMARKET                   VARCHAR(10)                  COMMENT 'Source market / point-of-sale code',
    CRUISELINECODE                 VARCHAR(10)       NOT NULL   COMMENT 'Cruise line code (part of PK)',
    SHIPCODE                       VARCHAR(10)       NOT NULL   COMMENT 'Ship code (part of PK)',
    CRUISELINENAME                 VARCHAR(200)                 COMMENT 'Cruise line display name',
    SOURCESHIPNAME                 VARCHAR(200)                 COMMENT 'Ship name as provided by source',
    EMBARKATIONDATE                TIMESTAMP_NTZ(9)  NOT NULL   COMMENT 'Embarkation date/time (part of PK)',
    DISEMBARKATIONDATE             TIMESTAMP_NTZ(9)             COMMENT 'Disembarkation date/time',
    SOURCETINERARYNAME             VARCHAR(200)                 COMMENT 'Itinerary name from source system',
    EMBARKATIONPORT                VARCHAR(200)                 COMMENT 'Embarkation port name (text)',
    EMBARKATIONPORTUN_LOCODE       VARCHAR(10)              COMMENT 'Embarkation UN/LOCODE (part of PK)',
    DISEMBARKATIONPORT             VARCHAR(200)                 COMMENT 'Disembarkation port name (text)',
    DISEMBARKATIONPORTUN_LOCODE    VARCHAR(10)         COMMENT 'Disembarkation UN/LOCODE (part of PK)',
    CRUISEDURATION                 NUMBER(18,0)        COMMENT 'Cruise duration in days (part of PK)',
    ITINERARYPORTCONTENT           VARCHAR(16777216)            COMMENT 'Concatenated/serialized sequence of itinerary ports (source payload)',
    UNLOCCODECONTENT               VARCHAR(16777216)            COMMENT 'Concatenated/serialized sequence of corresponding UN/LOCODEs',
    WEBSITE                        VARCHAR(250)                 COMMENT 'Source website/domain',
    CACHEPAGELINK                  VARCHAR(500)                 COMMENT 'Cached page link / source capture URL',
    TRADEHIERARCHYLVL1             VARCHAR(100)                 COMMENT 'Trade hierarchy level 1 mapping',
    TRADEHIERARCHYLVL2             VARCHAR(100)                 COMMENT 'Trade hierarchy level 2 mapping',
    TRADEHIERARCHYLVL3             VARCHAR(100)                 COMMENT 'Trade hierarchy level 3 mapping',
    FILEPATH                       VARCHAR(300)                 COMMENT 'Source file path / storage locator'

    -- CONSTRAINT PK_GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS PRIMARY KEY (
    --     RATEINQUIRYDATE,
    --     CRUISELINECODE,
    --     SHIPCODE,
    --     EMBARKATIONDATE,
    --     EMBARKATIONPORTUN_LOCODE,
    --     DISEMBARKATIONPORTUN_LOCODE,
    --     CRUISEDURATION
    -- )
)
COMMENT = 'Voyage-level product reference (RateGain pass): itinerary, ports, durations, and source lineage for RDV layer.';

insert into VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
select * from VESSOPS_D.L00_STG.GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS;

-- ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS
-- ADD CONSTRAINT PK_GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS PRIMARY KEY (
--     RATEINQUIRYDATE,
--     CRUISELINECODE,
--     SHIPCODE,
--     EMBARKATIONDATE,
--     EMBARKATIONPORTUN_LOCODE,
--     DISEMBARKATIONPORTUN_LOCODE,
--     CRUISEDURATION
-- );



ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
ADD COLUMN 
    VESSEL_IMO VARCHAR(50),
    NCLH_SHIP_CODE VARCHAR(5),
    SAIL_ID varchar(50),
    MD5_VOYAGE_HASH_KEY VARCHAR,
    HASH_DIFF VARCHAR,
    LDTS TIMESTAMP_NTZ(9),
    RCSR VARCHAR(200),
    LAST_MODIFIED_BY VARCHAR(500);

-- alter table VESSOPS_D.L00_STG.GCI_PRICING_CHECK_AK_1 
-- drop column SAIL_ID,MD5_VOYAGE_HASH_KEY,HASH_DIFF,LDTS,RCSR,LAST_MODIFIED_BY,VESSEL_IMO,NCLH_SHIP_CODE


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS AS a
SET VESSEL_IMO = b.VESSEL_IMO_NO, NCLH_SHIP_CODE= b.ship_code_3l
FROM VESSOPS_D.L10_RDV.gci_raw_ref_ship_data AS b
WHERE CONCAT(a.CRUISELINECODE, a.SHIPCODE) = b."5L_SHIP_CODE";



-- select top 10 * from VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS where md5_voyage_hash_key is not  null


-- UPDATE VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_LEVEL_RG_PASS
-- SET NCLH_SHIP_CODE = CASE 
--     -- Norwegian Cruise Line (NCL) Fleet
--     WHEN SOURCESHIPNAME IN ('Norwegian Spirit','NORWEGIAN SPIRIT') THEN 'SPR'
--     WHEN SOURCESHIPNAME IN ('Norwegian Sky','NORWEGIAN SKY') THEN 'SKY'
--     WHEN SOURCESHIPNAME IN ('Norwegian Star','NORWEGIAN STAR') THEN 'STA'
--     WHEN SOURCESHIPNAME IN ('Norwegian Sun','NORWEGIAN SUN') THEN 'SUN'
--     WHEN SOURCESHIPNAME IN ('Norwegian Dawn','NORWEGIAN DAWN') THEN 'DWN'
--     WHEN SOURCESHIPNAME IN ('Pride of America','PRIDE OF AMERICA','Pride Of America') THEN 'AME'
--     WHEN SOURCESHIPNAME IN ('Norwegian Jewel','NORWEGIAN JEWEL') THEN 'JWL'
--     WHEN SOURCESHIPNAME IN ('Norwegian Pearl','NORWEGIAN PEARL') THEN 'PRL'
--     WHEN SOURCESHIPNAME IN ('Norwegian Jade','NORWEGIAN JADE') THEN 'JAD'
--     WHEN SOURCESHIPNAME IN ('Norwegian Gem','NORWEGIAN GEM') THEN 'GEM'
--     WHEN SOURCESHIPNAME IN ('Norwegian Epic','NORWEGIAN EPIC') THEN 'EPC'
--     WHEN SOURCESHIPNAME IN ('Norwegian Breakaway','NORWEGIAN BREAKAWAY') THEN 'BWY'
--     WHEN SOURCESHIPNAME IN ('Norwegian Getaway','NORWEGIAN GETAWAY') THEN 'GWY'
--     WHEN SOURCESHIPNAME IN ('Norwegian Escape','NORWEGIAN ESCAPE') THEN 'ESC'
--     WHEN SOURCESHIPNAME IN ('Norwegian Joy','NORWEGIAN JOY','NCL JOY') THEN 'JOY'
--     WHEN SOURCESHIPNAME IN ('Norwegian Bliss','NORWEGIAN BLISS') THEN 'BLS'
--     WHEN SOURCESHIPNAME IN ('Norwegian Encore','NORWEGIAN ENCORE') THEN 'ENC'
--     WHEN SOURCESHIPNAME IN ('Norwegian Prima','NORWEGIAN PRIMA') THEN 'PRI'
--     WHEN SOURCESHIPNAME IN ('Norwegian Viva','NORWEGIAN VIVA') THEN 'VIV'
--     WHEN SOURCESHIPNAME IN ('Norwegian Aqua','NORWEGIAN AQUA') THEN 'AQU'
--     WHEN SOURCESHIPNAME IN ('Norwegian Luna','NORWEGIAN LUNA','lUNA') THEN 'LUN'
 
--     -- Oceania Cruises Fleet
--     WHEN SOURCESHIPNAME IN ('MS Insignia','Insignia','INSIGNIA') THEN 'INS'
--     WHEN SOURCESHIPNAME IN ('MS Regatta','Regatta','REGATTA') THEN 'REG'
--     WHEN SOURCESHIPNAME IN ('MS Sirena','Sirena','SIRENA') THEN 'SIR'
--     WHEN SOURCESHIPNAME IN ('Oceania Marina','Marina','MARINA') THEN 'MNA'
--     WHEN SOURCESHIPNAME IN ('Oceania Nautica','Nautica','NAUTICA') THEN 'NAU'
--     WHEN SOURCESHIPNAME IN ('Oceania Riviera','Riviera','RIVIERA') THEN 'RVA'
--     WHEN SOURCESHIPNAME IN ('Oceania Vista','Vista','VISTA') THEN 'VIS'
--     WHEN SOURCESHIPNAME IN ('Ocenia Allura','ALLURA') THEN 'ALU'
 
--     -- Regent Seven Seas Cruises Fleet
--     WHEN SOURCESHIPNAME IN ('Seven Seas Explorer','SEVEN SEAS EXPLORER') THEN 'EXP'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Mariner','SEVEN SEAS MARINER') THEN 'MAR'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Splendor','SEVEN SEAS SPLENDOR') THEN 'SPL'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Voyager','SEVEN SEAS VOYAGER') THEN 'VOY'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Navigator','SEVEN SEAS NAVIGATOR') THEN 'NAV'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Grandeur','SEVEN SEAS GRANDEUR') THEN 'GRA'
--     WHEN SOURCESHIPNAME IN ('Seven Seas Prestige','SEVEN SEAS PRESTIGE') THEN 'PRT'
 
--     ELSE NULL
-- END;


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
SET SAIL_ID = CONCAT(
    NCLH_SHIP_CODE,
    TO_CHAR(EMBARKATIONDATE, 'YYMMDD')
);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
SET MD5_VOYAGE_HASH_KEY = MD5(SAIL_ID);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
SET 
    LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

/* 2) Compute HASH_DIFF over all non-metadata columns */
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_LEVEL_RG_PASS
SET HASH_DIFF = TO_VARCHAR(
  MD5(
    CONCAT_WS(
      '|',
      COALESCE(TO_VARCHAR(RATEINQUIRYDATE), ''),
      COALESCE(SOURCEMARKET, ''),
      COALESCE(CRUISELINECODE, ''),
      COALESCE(SHIPCODE, ''),
      COALESCE(CRUISELINENAME, ''),
      COALESCE(SOURCESHIPNAME, ''),
      COALESCE(TO_VARCHAR(EMBARKATIONDATE), ''),
      COALESCE(TO_VARCHAR(DISEMBARKATIONDATE), ''),
      COALESCE(SOURCETINERARYNAME, ''),
      COALESCE(EMBARKATIONPORT, ''),
      COALESCE(EMBARKATIONPORTUN_LOCODE, ''),
      COALESCE(DISEMBARKATIONPORT, ''),
      COALESCE(DISEMBARKATIONPORTUN_LOCODE, ''),
      COALESCE(TO_VARCHAR(CRUISEDURATION), ''),
      COALESCE(ITINERARYPORTCONTENT, ''),
      COALESCE(UNLOCCODECONTENT, ''),
      COALESCE(WEBSITE, ''),
      COALESCE(CACHEPAGELINK, ''),
      COALESCE(TRADEHIERARCHYLVL1, ''),
      COALESCE(TRADEHIERARCHYLVL2, ''),
      COALESCE(TRADEHIERARCHYLVL3, ''),
      COALESCE(FILEPATH, '')
    )
  )
);

-- select top 10 * from VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
/* =========================
   DDL: Day-level voyage table
   ========================= */

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS 
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS 


CREATE OR REPLACE TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS (
    RATEINQUIRYDATE              TIMESTAMP_NTZ(9)  NOT NULL COMMENT 'Shop/inquiry timestamp from source (part of PK)',
    SOURCEMARKET                 VARCHAR(10)                 COMMENT 'Source market / point-of-sale code',
    CRUISELINECODE               VARCHAR(10)       NOT NULL  COMMENT 'Cruise line code (part of PK)',
    SHIPCODE                     VARCHAR(10)       NOT NULL  COMMENT 'Ship code (part of PK)',

    EMBARKATIONPORT              VARCHAR(200)                COMMENT 'Embarkation port name (text)',
    EMBARKATIONPORTUN_LOCODE     VARCHAR(10)       NOT NULL  COMMENT 'Embarkation UN/LOCODE (part of PK)',
    DISEMBARKATIONPORT           VARCHAR(200)                COMMENT 'Disembarkation port name (text)',
    DISEMBARKATIONPORTUN_LOCODE  VARCHAR(10)       NOT NULL  COMMENT 'Disembarkation UN/LOCODE (part of PK)',
    CRUISEDURATION               NUMBER(18,0)      NOT NULL  COMMENT 'Cruise duration in days (part of PK)',

    CRUISELINENAME               VARCHAR(200)                COMMENT 'Cruise line display name',
    SOURCESHIPNAME               VARCHAR(200)                COMMENT 'Ship name as provided by source',
    WEBSITE                      VARCHAR(200)                COMMENT 'Source website/domain',
    CACHEPAGELINK                VARCHAR(300)                COMMENT 'Cached page link / source capture URL',

    EMBARKATIONDATE              TIMESTAMP_NTZ(9)  NOT NULL  COMMENT 'Embarkation date/time (part of PK)',
    DISEMBARKATIONDATE           TIMESTAMP_NTZ(9)            COMMENT 'Disembarkation date/time',

    ARRIVAL_TIM                  TIME(7)                     COMMENT 'Planned arrival time at port (local, source)',
    DEPARTURE_TIM                TIME(7)                     COMMENT 'Planned departure time from port (local, source)',

    PORTUN_LOCODE                VARCHAR(10)                 COMMENT 'UN/LOCODE for the call port (this row)',
    PORT_CITY_NAM                VARCHAR(200)                COMMENT 'Port city name',
    PORT_COUNTRY_NAM             VARCHAR(200)                COMMENT 'Port country name',

    ITINERARY_DATE               TIMESTAMP_NTZ(9)  NOT NULL  COMMENT 'Calendar date for this itinerary day (part of PK)',
    ITINERARY_DAY                NUMBER(18,0)                COMMENT 'Itinerary day number within the voyage (1..N)',
    SEQUENCE_ID                  NUMBER(18,0)      NOT NULL  COMMENT 'Sequence/order of the call within the voyage (part of PK)',

    TRADEHIERARCHYLVL1           VARCHAR(100)                COMMENT 'Trade hierarchy level 1 mapping',
    TRADEHIERARCHYLVL2           VARCHAR(100)                COMMENT 'Trade hierarchy level 2 mapping',
    TRADEHIERARCHYLVL3           VARCHAR(100)                COMMENT 'Trade hierarchy level 3 mapping',
    FILEPATH                     VARCHAR(300)                COMMENT 'Source file path / storage locator'

    CONSTRAINT PK_GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS PRIMARY KEY (
        RATEINQUIRYDATE,
        CRUISELINECODE,
        SHIPCODE,
        EMBARKATIONDATE,
        EMBARKATIONPORTUN_LOCODE,
        DISEMBARKATIONPORTUN_LOCODE,
        CRUISEDURATION,
        ITINERARY_DATE,
        SEQUENCE_ID
    )
)
COMMENT = 'Day-level voyage reference (RateGain pass): per-day port calls and timings with voyage context for RDV layer.';

insert into VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
select * from VESSOPS_D.L00_STG.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASs

ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
ADD CONSTRAINT PK_GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS PRIMARY KEY (
        RATEINQUIRYDATE,
        CRUISELINECODE,
        SHIPCODE,
        EMBARKATIONDATE,
        EMBARKATIONPORTUN_LOCODE,
        DISEMBARKATIONPORTUN_LOCODE,
        CRUISEDURATION,
        ITINERARY_DATE,
        SEQUENCE_ID
);


-- update VESSOPS_D.L00_STG.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS_V1
-- set shipcode='NA'
-- where shipcode is null and upper(trim(cruiselinecode)) in ('HAL','SIL')


-- insert into VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
-- select *  from VESSOPS_D.L00_STG.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS_V1


ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
ADD COLUMN 
    VESSEL_IMO VARCHAR(50),
    NCLH_SHIP_CODE VARCHAR(5),
    SAIL_ID varchar(50),
    MD5_VOYAGE_HASH_KEY VARCHAR,
    HASH_DIFF VARCHAR,
    LDTS TIMESTAMP_NTZ(9),
    RCSR VARCHAR(200),
    LAST_MODIFIED_BY VARCHAR;

-- alter table VESSOPS_D.L00_STG.GCI_PRICING_CHECK_AK_1 
-- drop column SAIL_ID,MD5_VOYAGE_HASH_KEY,HASH_DIFF,LDTS,RCSR,LAST_MODIFIED_BY,VESSEL_IMO,NCLH_SHIP_CODE


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS AS a
SET VESSEL_IMO = b.VESSEL_IMO_NO, NCLH_SHIP_CODE= b.ship_code_3l
FROM VESSOPS_D.L10_RDV.gci_raw_ref_ship_data AS b
WHERE CONCAT(a.CRUISELINECODE, a.SHIPCODE) = b."5L_SHIP_CODE";
    


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
SET SAIL_ID = CONCAT(
    NCLH_SHIP_CODE,
    TO_CHAR(EMBARKATIONDATE, 'YYMMDD')
);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
SET MD5_VOYAGE_HASH_KEY = MD5(SAIL_ID);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
SET 
    LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

/* Build a deterministic HASH_DIFF across business columns (excludes any metadata) */
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS
SET HASH_DIFF = TO_VARCHAR(
  MD5(
    CONCAT_WS(
      '|',
      /* timestamps with explicit formatting for determinism */
      COALESCE(TO_VARCHAR(RATEINQUIRYDATE,   'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
      COALESCE(SOURCEMARKET, ''),
      COALESCE(CRUISELINECODE, ''),
      COALESCE(SHIPCODE, ''),

      COALESCE(EMBARKATIONPORT, ''),
      COALESCE(EMBARKATIONPORTUN_LOCODE, ''),
      COALESCE(DISEMBARKATIONPORT, ''),
      COALESCE(DISEMBARKATIONPORTUN_LOCODE, ''),
      COALESCE(TO_VARCHAR(CRUISEDURATION), ''),

      COALESCE(CRUISELINENAME, ''),
      COALESCE(SOURCESHIPNAME, ''),
      COALESCE(WEBSITE, ''),
      COALESCE(CACHEPAGELINK, ''),

      COALESCE(TO_VARCHAR(EMBARKATIONDATE,  'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
      COALESCE(TO_VARCHAR(DISEMBARKATIONDATE,'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),

      /* times formatted explicitly */
      COALESCE(TO_VARCHAR(ARRIVAL_TIM,   'HH24:MI:SS.FF9'), ''),
      COALESCE(TO_VARCHAR(DEPARTURE_TIM, 'HH24:MI:SS.FF9'), ''),

      COALESCE(PORTUN_LOCODE, ''),
      COALESCE(PORT_CITY_NAM, ''),
      COALESCE(PORT_COUNTRY_NAM, ''),

      COALESCE(TO_VARCHAR(ITINERARY_DATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), ''),
      COALESCE(TO_VARCHAR(ITINERARY_DAY), ''),
      COALESCE(TO_VARCHAR(SEQUENCE_ID), ''),

      COALESCE(TRADEHIERARCHYLVL1, ''),
      COALESCE(TRADEHIERARCHYLVL2, ''),
      COALESCE(TRADEHIERARCHYLVL3, ''),
      COALESCE(FILEPATH, '')
    )
  )
);



CREATE OR REPLACE TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS (
    RATEINQUIRYDATE           TIMESTAMP_NTZ(9) NOT NULL,
    SOURCEMARKET              VARCHAR(10),
    CRUISELINECODE            VARCHAR(10)      NOT NULL,
    SHIPCODE                  VARCHAR(10)      NOT NULL,
    CRUISELINENAME            VARCHAR(255)     NOT NULL,
    SOURCESHIPNAME            VARCHAR(255)     NOT NULL,
    WEBSITE                   VARCHAR(255),
    CACHEPAGELINK             VARCHAR(500),
    CABINTYPE                 VARCHAR(20),
    CATEGORYCODE              VARCHAR(10),
    SOURCECURRENCY            VARCHAR(10),
    FARE_PASSENGERTYPE        VARCHAR(15),
    ORIGCURRENCY              VARCHAR(10),
    RATEPPUSD                 NUMBER(18,2),
    TAXESANDFEES              NUMBER(18,2),
    TAXESANDFEESUSD           NUMBER(18,2),
    CONVERTIONRATE            NUMBER(18,6),
    CRUISETYPE                VARCHAR(28),
    CRUISEDURATION            NUMBER(5,0),
    DISEMBARKDATE             TIMESTAMP_NTZ(9),
    SHOPFREQUENCY             VARCHAR(50),
    EMBARKATIONDATE           TIMESTAMP_NTZ(9),
    SOURCEITINERARYNAME       VARCHAR(800),
    EMBARKATIONPORT           VARCHAR(255),
    EMBARKATIONPORTUN_LOCODE  VARCHAR(20),
    DISEMBARKATIONPORT        VARCHAR(255),
    DISEMBARKATIONPORTUN_LOCODE VARCHAR(20),
    ITINERARYPORTCONTENT      VARCHAR(16777216),
    RATETYPE                  VARCHAR(20),
    TAXINCLUSIVERATEPP        NUMBER(18,2),
    TAXEXCLUSIVERATEPP        NUMBER(18,2),
    AVAILABLEOFFER            VARCHAR(16777216),
    LASTSHOPDATE              TIMESTAMP_NTZ(9),
    LASTSHOPPEDSUBCATEGORY    VARCHAR(20),
    AVAILABILITY              VARCHAR(20),
    OBSTRUCTED_Y_N            VARCHAR(10),
    FILEPATH                  VARCHAR(500),
    GRATUITY_Y_N              VARCHAR(10),
    NCL_CABINTYPE             VARCHAR(500),
    RSS_CABINTYPE             VARCHAR(500),
    OCI_CABINTYPE             VARCHAR(500),

    -- Constraints
    CONSTRAINT PK_GCI_RAW_PRICING PRIMARY KEY (
        RATEINQUIRYDATE, CRUISELINECODE, SHIPCODE, EMBARKATIONDATE,EMBARKATIONPORTUN_LOCODE,DISEMBARKATIONPORTUN_LOCODE,CRUISEDURATION, CABINTYPE,RATETYPE,AVAILABILITY
    )
);

insert into VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
select * from VESSOPS_D.L00_STG.GCI_RAW_PRICING_RG_PASS;



-- ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_PRICING_RG_PASS 
-- DROP CONSTRAINT PK_GCI_RAW_PRICING;

-- ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_PRICING_RG_PASS
-- ADD CONSTRAINT PK_GCI_RAW_PRICING
-- PRIMARY KEY (
--     RATEINQUIRYDATE, 
--     CRUISELINECODE, 
--     SHIPCODE, 
--     EMBARKATIONDATE,
--     EMBARKATIONPORTUN_LOCODE,
--     DISEMBARKATIONPORTUN_LOCODE,
--     CRUISEDURATION,
--     CABINTYPE,
--     RATETYPE,
--     AVAILABILITY,
--     TAXINCLUSIVERATEPP
-- ) NOT ENFORCED;



ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
ADD COLUMN 
    VESSEL_IMO VARCHAR(50),
    NCLH_SHIP_CODE VARCHAR(5),
    SAIL_ID varchar(50),
    MD5_VOYAGE_HASH_KEY VARCHAR,
    HASH_DIFF VARCHAR,
    LDTS TIMESTAMP_NTZ(9),
    RCSR VARCHAR(200),
    LAST_MODIFIED_BY VARCHAR;

-- -- alter table VESSOPS_D.L00_STG.GCI_PRICING_CHECK_AK_1 
-- -- drop column SAIL_ID,MD5_VOYAGE_HASH_KEY,HASH_DIFF,LDTS,RCSR,LAST_MODIFIED_BY,VESSEL_IMO,NCLH_SHIP_CODE

-- alter table VESSOPS_D.L10_RDV.gci_raw_ref_ship_data
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA;

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS AS a
SET VESSEL_IMO = b.VESSEL_IMO_NO,NCLH_SHIP_CODE= b.ship_code_3l
FROM VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_SHIP_DATA AS b
WHERE CONCAT(a.CRUISELINECODE, a.SHIPCODE) = b."5L_SHIP_CODE";
    

-- -- select top 10 * from VESSOPS_D.L00_STG.GCI_PRICING_CHECK_AK_1 where md5_voyage_hash_key is not  null


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
SET SAIL_ID = CONCAT(
    NCLH_SHIP_CODE,
    TO_CHAR(EMBARKATIONDATE, 'YYMMDD')
);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
SET MD5_VOYAGE_HASH_KEY = MD5(SAIL_ID);


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
SET HASH_DIFF = MD5(
    CONCAT_WS(
        '|',
        COALESCE(TO_VARCHAR(RATEINQUIRYDATE), ''),
        COALESCE(SOURCEMARKET, ''),
        COALESCE(CRUISELINECODE, ''),
        COALESCE(SHIPCODE, ''),
        COALESCE(CRUISELINENAME, ''),
        COALESCE(SOURCESHIPNAME, ''),
        COALESCE(WEBSITE, ''),
        COALESCE(CACHEPAGELINK, ''),
        COALESCE(CABINTYPE, ''),
        COALESCE(CATEGORYCODE, ''),
        COALESCE(SOURCECURRENCY, ''),
        COALESCE(FARE_PASSENGERTYPE, ''),
        COALESCE(ORIGCURRENCY, ''),
        COALESCE(TO_VARCHAR(RATEPPUSD), ''),
        COALESCE(TO_VARCHAR(TAXESANDFEES), ''),
        COALESCE(TO_VARCHAR(TAXESANDFEESUSD), ''),
        COALESCE(TO_VARCHAR(CONVERTIONRATE), ''),
        COALESCE(CRUISETYPE, ''),
        COALESCE(TO_VARCHAR(CRUISEDURATION), ''),
        COALESCE(TO_VARCHAR(DISEMBARKDATE), ''),
        COALESCE(SHOPFREQUENCY, ''),
        COALESCE(TO_VARCHAR(EMBARKATIONDATE), ''),
        COALESCE(SOURCEITINERARYNAME, ''),
        COALESCE(EMBARKATIONPORT, ''),
        COALESCE(EMBARKATIONPORTUN_LOCODE, ''),
        COALESCE(DISEMBARKATIONPORT, ''),
        COALESCE(DISEMBARKATIONPORTUN_LOCODE, ''),
        COALESCE(ITINERARYPORTCONTENT, ''),
        COALESCE(RATETYPE, ''),
        COALESCE(TO_VARCHAR(TAXINCLUSIVERATEPP), ''),
        COALESCE(TO_VARCHAR(TAXEXCLUSIVERATEPP), ''),
        COALESCE(AVAILABLEOFFER, ''),
        COALESCE(TO_VARCHAR(LASTSHOPDATE), ''),
        COALESCE(LASTSHOPPEDSUBCATEGORY, ''),
        COALESCE(AVAILABILITY, ''),
        COALESCE(OBSTRUCTED_Y_N, ''),
        COALESCE(FILEPATH, ''),
        COALESCE(GRATUITY_Y_N, ''),
        COALESCE(NCL_CABINTYPE, ''),
        COALESCE(RSS_CABINTYPE, ''),
        COALESCE(OCI_CABINTYPE, '')
    )
);


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_PRICING_RG_PASS
SET 
    LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

--select count(*) from VESSOPS_D.L10_RDV.GCI_RAW_PRODUCT_VOY_DAY_LEVEL_RG_PASS where nclh_ship_code is null


--select  top 10 * from VESSOPS_D.L10_RDV.GCI_RAW_REF_SHIP_DATA

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_MERGED_SHIP_DATA
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_MERGED_PORT_DATA
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_GRAND_VOYAGES
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_MANUAL_SCR_CRUISE
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_CALENDAR_TABLE
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE;

-- alter table VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY
-- rename to VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_TRADE_MARKET_HIRERACHY;

create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_MERGED_SHIP_DATA;

select count(*) from SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA

/* --- GCI_RAW_REF_MERGED_SHIP_DATA --- */
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS VESSEL_IMO_NO VARCHAR;
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS MD5_SHIP_HASH_KEY VARCHAR;
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS HASH_DIFF VARCHAR;
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_SHIP_DATA ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);



UPDATE VESSOPS_D.L10_RDV.GCI_RAW_NCH_REF_MERGED_SHIP_DATA a
SET a.VESSEL_IMO_NO = b.VESSEL_IMO_NO
FROM VESSOPS_D.L10_RDV.GCI_RAW_REF_SHIP_DATA b
WHERE UPPER(TRIM(CONCAT(a.mp_cruiselinecode, a.mp_shipcode))) = UPPER(TRIM(b."5L_SHIP_CODE"));


-- select top 10 * from gci_raw_ref_ship_data
-- select top 0 * from VESSOPS_D.L10_RDV.GCI_RAW_REF_MERGED_SHIP_DATA
UPDATE VESSOPS_D.L10_RDV.GCI_RAW_NCH_REF_MERGED_SHIP_DATA
set md5_shiP_HASH_KEY=md5(VESSEL_IMO_NO)

UPDATE VESSOPS_D.L10_RDV.GCI_RAW_NCH_REF_MERGED_SHIP_DATA
SET HASH_DIFF = MD5(
      COALESCE(CRUISELINENAME,'') || '|' ||
      COALESCE(CRUISELINECODE,'') || '|' ||
      COALESCE(SOURCESHIPNAME,'') || '|' ||
      COALESCE(SHIPCODE,'') || '|' ||
      COALESCE(MP_CRUISELINENAME,'') || '|' ||
      COALESCE(TB_CRUISELINENAME,'') || '|' ||
      COALESCE(MP_CRUISELINECODE,'') || '|' ||
      COALESCE(MP_SOURCESHIPNAME,'') || '|' ||
      COALESCE(MP_SHIPCODE,'') || '|' ||
      COALESCE(VESSEL_IMO_NO,'')
   );

   
UPDATE VESSOPS_D.L10_RDV.GCI_RAW_NCH_REF_MERGED_SHIP_DATA
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_NCH_REF_MERGED_SHIP_DATA
ADD CONSTRAINT PK_GCI_RAW_REF_MERGED_SHIP_DATA
PRIMARY KEY (MP_CRUISELINENAME, MP_SOURCESHIPNAME);


create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_MERGED_PORT_DATA;  


-- select top 0 * from VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA --where dtl_port_city_nam='DTL_PORT_CITY_NAM'

/* --- GCI_RAW_REF_MERGED_PORT_DATA --- */

ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA ADD COLUMN IF NOT EXISTS MD5_PORT_HASH_KEY VARCHAR;
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA ADD COLUMN IF NOT EXISTS HASH_DIFF VARCHAR;
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();

update VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA 
set MD5_PORT_HASH_KEY =md5(TB_UNV_PORT_CODE);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA
SET HASH_DIFF = MD5(
      COALESCE(DTL_PORT_CITY_NAM,'') || '|' ||
      COALESCE(DTL_PORTUN_LOCODE,'') || '|' ||
      COALESCE(TB_PORT_LOC_NAME,'')  || '|' ||
      COALESCE(TB_UNV_PORT_CODE,'')  || '|' ||
      COALESCE(TB_COUNTRY_NAME,'')
   );

ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MERGED_PORT_DATA
ADD CONSTRAINT PK_GCI_RAW_REF_MERGED_PORT_DATA
PRIMARY KEY (TB_UNV_PORT_CODE, DTL_PORT_CITY_NAM);




create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_GRAND_VOYAGES;   


/* --- GCI_RAW_REF_GRAND_VOYAGES --- */
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES ADD COLUMN IF NOT EXISTS HASH_DIFF VARCHAR();
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'MITCH',
    LAST_MODIFIED_BY = CURRENT_USER();
    
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES
SET HASH_DIFF = MD5(
      COALESCE(CRUISE,'')   || '|' ||
      COALESCE(FMDATE,'')   || '|' ||
      COALESCE(TODATE,'')   || '|' ||
      COALESCE(DAYS,'')     || '|' ||
      COALESCE(WORLDCR,'')  || '|' ||
      COALESCE(SHIP,'')     || '|' ||
      COALESCE(SUPERMKT,'')
   );

ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_GRAND_VOYAGES
ADD CONSTRAINT PK_GCI_RAW_REF_GRAND_VOYAGES
PRIMARY KEY (CRUISE);


create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_MANUAL_SCR_CRUISE;


-- select * from VESSOPS_D.L00_STG.GCI_RAW_REF_MANUAL_SCR_CRUISE


/* --- GCI_RAW_REF_MANUAL_SCR_CRUISE --- */
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_MANUAL_SCR_CRUISE
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'RATEGAIN',
    LAST_MODIFIED_BY = CURRENT_USER();


/* --- GCI_RAW_REF_CALENDAR_TABLE --- */
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE ADD COLUMN IF NOT EXISTS HASH_DIFF VARCHAR();
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'CALENDAR_DATA',
    LAST_MODIFIED_BY = CURRENT_USER();

ALTER TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE
ADD CONSTRAINT PK_GCI_RAW_REF_CALENDAR
PRIMARY KEY (CALENDAR_DATE);

UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE
SET HASH_DIFF = MD5(
    COALESCE(CALENDAR_DATE,'')             || '|' ||
    COALESCE(CALENDAR_DATE_STRING,'')      || '|' ||
    COALESCE(CALENDAR_MONTH,'')            || '|' ||
    COALESCE(CALENDAR_DAY,'')              || '|' ||
    COALESCE(CALENDAR_YEAR,'')             || '|' ||
    COALESCE(CALENDAR_QUARTER,'')          || '|' ||
    COALESCE(DAY_NAME,'')                  || '|' ||
    COALESCE(DAY_OF_WEEK,'')               || '|' ||
    COALESCE(DAY_OF_WEEK_IN_MONTH,'')      || '|' ||
    COALESCE(DAY_OF_WEEK_IN_YEAR,'')       || '|' ||
    COALESCE(DAY_OF_WEEK_IN_QUARTER,'')    || '|' ||
    COALESCE(DAY_OF_QUARTER,'')            || '|' ||
    COALESCE(DAY_OF_YEAR,'')               || '|' ||
    COALESCE(WEEK_OF_MONTH,'')             || '|' ||
    COALESCE(WEEK_OF_QUARTER,'')           || '|' ||
    COALESCE(WEEK_OF_YEAR,'')              || '|' ||
    COALESCE(MONTH_NAME,'')                || '|' ||
    COALESCE(FIRST_DATE_OF_WEEK,'')        || '|' ||
    COALESCE(LAST_DATE_OF_WEEK,'')         || '|' ||
    COALESCE(FIRST_DATE_OF_MONTH,'')       || '|' ||
    COALESCE(LAST_DATE_OF_MONTH,'')        || '|' ||
    COALESCE(FIRST_DATE_OF_QUARTER,'')     || '|' ||
    COALESCE(LAST_DATE_OF_QUARTER,'')      || '|' ||
    COALESCE(FIRST_DATE_OF_YEAR,'')        || '|' ||
    COALESCE(LAST_DATE_OF_YEAR,'')         || '|' ||
    COALESCE(IS_LEAP_YEAR,'')              || '|' ||
    COALESCE(DAYS_IN_MONTH,'')             || '|' ||
    COALESCE(MERCHANT_DAY_OF_YEAR,'')      || '|' ||
    COALESCE(MERCHANT_WEEK_OF_YEAR,'')     || '|' ||
    COALESCE(MERCHANT_CALENDAR_MONTH,'')   || '|' ||
    COALESCE(MERCHANT_CALENDAR_QUARTER,'') || '|' ||
    COALESCE(MERCHANT_FIRST_DATE_OF_WEEK,'')   || '|' ||
    COALESCE(MERCHANT_LAST_DATE_OF_WEEK,'')    || '|' ||
    COALESCE(MERCHANT_FIRST_DATE_OF_MONTH,'')  || '|' ||
    COALESCE(MERCHANT_LAST_DATE_OF_MONTH,'')   || '|' ||
    COALESCE(MERCHANT_FIRST_DATE_OF_QUARTER,'')|| '|' ||
    COALESCE(MERCHANT_LAST_DATE_OF_QUARTER,'') || '|' ||
    COALESCE(MERCHANT_FIRST_DATE_OF_YEAR,'')   || '|' ||
    COALESCE(MERCHANT_LAST_DATE_OF_YEAR,'')    || '|' ||
    COALESCE(ISO_DAY_OF_WEEK,'')           || '|' ||
    COALESCE(ISO_DAY_OF_YEAR,'')           || '|' ||
    COALESCE(ISO_WEEK_OF_YEAR,'')          || '|' ||
    COALESCE(ISO_FIRST_DATE_OF_WEEK,'')    || '|' ||
    COALESCE(ISO_LAST_DATE_OF_WEEK,'')     || '|' ||
    COALESCE(ISO_STRING,'')                || '|' ||
    COALESCE(FISCAL_DAY_OF_YEAR,'')        || '|' ||
    COALESCE(FISCAL_WEEK_OF_YEAR,'')       || '|' ||
    COALESCE(FISCAL_QUARTER,'')            || '|' ||
    COALESCE(FISCAL_MONTH,'')              || '|' ||
    COALESCE(FISCAL_FIRST_DATE_OF_YEAR,'') || '|' ||
    COALESCE(FISCAL_LAST_DATE_OF_YEAR,'')  || '|' ||
    COALESCE(SEASON,'')
);




create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_TRADE_MARKET_HIRERACHY  as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_TRADE_MARKET_HIRERACHY;


/* --- GCI_RAW_REF_TRADE_MARKET_HIRERACHY --- */
ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY ADD COLUMN IF NOT EXISTS HASH_DIFF VARCHAR();
ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY ADD COLUMN IF NOT EXISTS LDTS TIMESTAMP_NTZ(9);
ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY ADD COLUMN IF NOT EXISTS RCSR VARCHAR(200);
ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY ADD COLUMN IF NOT EXISTS LAST_MODIFIED_BY VARCHAR(500);


UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_TRADE_MARKET_HIRERACHY
SET LDTS = CURRENT_TIMESTAMP(),
    RCSR = 'MITCH',
    LAST_MODIFIED_BY = CURRENT_USER();
    
-- -- 2️⃣ Add Primary Key
ALTER TABLE VESSOPS_D.L10_RDV.GCI_RAW_REF_TRADE_MARKET_HIRERACHY 
ADD CONSTRAINT PK_GCI_RAW_REF_TRADE_MARKET_HIRERACHY 
PRIMARY KEY (NCLH_RM_TRADE1, NCLH_RM_TRADE2, "MS_recommended_T3");

-- -- 3️⃣ Update HASH_DIFF with MD5 hash
UPDATE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_TRADE_MARKET_HIRERACHY
SET HASH_DIFF = MD5(
    COALESCE(NCLH_RM_TRADE1, '') || '|' ||
    COALESCE(NCLH_RM_TRADE2, '') || '|' ||
    COALESCE("MS_recommended_T3", '') || '|' ||
    COALESCE("Market 1", '') || '|' ||
    COALESCE("Market 2", '') || '|' ||
    COALESCE("Trade_3_MS", '') || '|' ||
    COALESCE("Trade_4_MS", '')
);



create or replace TABLE VESSOPS_D.L10_RDV.SAT_GCI_NCH_RAW_REF_CALENDAR_TABLE as 
select * from VESSOPS_D.L00_STG.GCI_RAW_REF_CALENDAR_TABLE;  



