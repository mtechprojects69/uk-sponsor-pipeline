-- Silver layer: normalized sponsor data
-- Cleans casing/whitespace inconsistencies from Bronze (e.g. "London" vs "london")

SELECT
  TRIM(INITCAP(organisation_name)) AS organisation_name,
  TRIM(INITCAP(town_city)) AS town_city,
  TRIM(INITCAP(county)) AS county,
  TRIM(type_rating) AS type_rating,
  TRIM(route) AS route,
  loaded_at
FROM `uk-sponsor-pipeline.uk_sponsor_pipeline.raw_sponsors`
