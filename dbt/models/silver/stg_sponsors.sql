-- Silver layer: normalized + enriched sponsor data
-- Builds on Bronze (raw_sponsors) by:
--   1. Normalizing casing/whitespace (organisation_name, town_city, county)
--   2. Splitting type_rating into sponsor_type + sponsor_rating
--   3. Extracting "trading as" names separately from legal names
--   4. Adding name_for_matching: a cleaned join key for Companies House matching
--   5. Deduplicating exact repeat rows
--   6. Adding a stable sponsor_id hash for day-over-day change tracking

CREATE OR REPLACE VIEW `uk-sponsor-pipeline.uk_sponsor_pipeline_silver.stg_sponsors` AS
SELECT DISTINCT
  TRIM(INITCAP(organisation_name)) AS organisation_name,

  -- Cleaned name for matching against Companies House: uppercase,
  -- legal suffixes stripped, punctuation removed
  UPPER(TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(organisation_name, r'\s+(LIMITED|LTD|PLC)\.?$', ''),
    r'[^A-Za-z0-9 ]', ''
  ))) AS name_for_matching,

  -- "t/as" trading name, if present
  REGEXP_EXTRACT(organisation_name, r't/as?\s+(.*)$') AS trading_as,

  TRIM(INITCAP(town_city)) AS town_city,
  TRIM(INITCAP(county)) AS county,

  -- Split "Worker (A rating)" into sponsor_type + sponsor_rating
  REGEXP_EXTRACT(type_rating, r'^(.*?)\s*\(') AS sponsor_type,
  REGEXP_EXTRACT(type_rating, r'\((.*?)\s*rating\)') AS sponsor_rating,

  TRIM(route) AS route,
  loaded_at,

  -- Stable ID for tracking the same sponsor across days
  TO_HEX(MD5(CONCAT(organisation_name, IFNULL(town_city, ''), route))) AS sponsor_id

FROM `uk-sponsor-pipeline.uk_sponsor_pipeline.raw_sponsors`
