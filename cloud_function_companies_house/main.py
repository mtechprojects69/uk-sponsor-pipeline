"""
Cloud Function: enrich_companies_house

Purpose: enrich sponsor records with official Companies House data,
filtered down to IT / tech companies specifically.

Approach:
1. Read distinct sponsor names (name_for_matching) from the Silver view.
2. Skip any sponsor already attempted before (matched or not) — tracked
   in a separate "attempts" table, so re-runs never repeat work.
3. For each remaining sponsor:
   a. Call the Companies House SEARCH API to find the company number.
   b. Call the Companies House PROFILE API (using that number) to get
      full details, including sic_codes — search alone doesn't return
      industry classification, only profile does.
   c. Check the returned sic_codes against a known list of IT/tech SIC
      codes. Only IT/tech companies get written to raw_companies_house.
4. Every sponsor is logged in companies_house_attempts regardless of
   industry (matched-but-not-tech counts as "attempted"), so re-runs
   never re-query the same non-tech company repeatedly.

Trigger: HTTP (can be scheduled, e.g. weekly, since company registration
details don't change daily the way the sponsor register does).

Rate limiting: Companies House allows 600 requests per 5 minutes.
Since this version makes TWO calls per sponsor (search + profile),
effective throughput is roughly half of the single-call version —
pacing and batch size are set with this in mind.
"""

import os
import time
from datetime import datetime, timezone

import requests
import functions_framework
from google.cloud import bigquery
import logging

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
REQUEST_DELAY_SECONDS = float(
    os.environ.get("REQUEST_DELAY_SECONDS", "0.6")
)
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

logging.basicConfig(level=logging.INFO)

CH_API_KEY = os.environ.get("COMPANIES_HOUSE_API_KEY")
CH_SEARCH_URL = "https://api.company-information.service.gov.uk/search/companies"
CH_PROFILE_URL = "https://api.company-information.service.gov.uk/company/{company_number}"

BQ_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "uk_sponsor_pipeline")
SILVER_DATASET = os.environ.get("SILVER_DATASET", "uk_sponsor_pipeline_silver")
CH_TABLE = os.environ.get("CH_TABLE", "raw_companies_house")
ATTEMPTS_TABLE = os.environ.get("ATTEMPTS_TABLE", "companies_house_attempts")

# How many sponsors to process per run — keeps each invocation fast and
# within Cloud Functions' request timeout. Re-running the function
# picks up where it left off, since already-attempted sponsors are skipped.
BATCH_SIZE = int(os.environ.get("CH_BATCH_SIZE", "150"))

# Seconds to wait between API calls, to stay comfortably under
# Companies House's 600-requests-per-5-minutes limit. We make 2 calls
# per sponsor (search + profile), so this delay applies between each
# individual HTTP call, not per sponsor.
REQUEST_DELAY_SECONDS = 0.6

# UK SIC 2007 codes covering IT / software / tech services.
# Reference: gov.uk SIC code list, "Information and communication" section.
TECH_SIC_CODES = {
    "58201",  # Publishing of computer games
    "58202",  # Other software publishing
    "62011",  # Ready-made interactive leisure and entertainment software development
    "62012",  # Business and domestic software development
    "62020",  # Information technology consultancy activities
    "62030",  # Computer facilities management activities
    "62090",  # Other information technology and computer service activities
    "63110",  # Data processing, hosting and related activities
    "63120",  # Web portals
    "63990",  # Other information service activities n.e.c.
    "26200",  # Manufacture of computers and peripheral equipment
    "46510",  # Wholesale of computers, computer peripheral equipment and software
    "47410",  # Retail sale of computers, peripheral units and software in specialised stores
    "95110",  # Repair of computers and peripheral equipment
}


def is_tech_company(sic_codes: list) -> bool:
    if not sic_codes:
        return False
    return any(code in TECH_SIC_CODES for code in sic_codes)


def get_unattempted_sponsors(client: bigquery.Client, limit: int):
    """Return sponsor names from Silver that haven't been attempted yet
    (matched or not) — checked against the attempts log, not the matches
    table, since matches-only means a NOT_FOUND sponsor would otherwise
    be retried forever."""
    query = f"""
        SELECT DISTINCT s.name_for_matching
        FROM `{BQ_PROJECT}.{SILVER_DATASET}.stg_sponsors` s
        LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.{ATTEMPTS_TABLE}` a
          ON s.name_for_matching = a.name_for_matching
        WHERE a.name_for_matching IS NULL
          AND s.name_for_matching IS NOT NULL
          AND s.name_for_matching != ''
        LIMIT {limit}
    """
    return [row.name_for_matching for row in client.query(query).result()]


def search_company(name: str):

    response = companies_house_get(
        CH_SEARCH_URL,
        {
            "q": name,
            "items_per_page": 1,
        },
    )

    if response is None:
        return None

    items = response.json().get("items", [])

    if not items:
        return None

    return items[0]


def get_company_profile(company_number: str):

    response = companies_house_get(
        CH_PROFILE_URL.format(company_number=company_number)
    )

    if response is None:
        return None

    return response.json()


def _write_json_rows(client: bigquery.Client, table_ref: str, rows: list, schema: list):
    import io
    import json

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    buffer = io.StringIO()
    for row in rows:
        buffer.write(json.dumps(row) + "\n")
    buffer.seek(0)

    load_job = client.load_table_from_file(buffer, table_ref, job_config=job_config)
    load_job.result()

def companies_house_get(url, params=None):
    """
    Wrapper para chamadas à API do Companies House
    com retry exponencial.
    """

    backoff = 2

    for attempt in range(MAX_RETRIES):

        try:

            response = requests.get(
                url,
                params=params,
                auth=(CH_API_KEY, ""),
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 200:
                return response

            if response.status_code in [429, 500, 502, 503, 504]:

                retry_after = response.headers.get("Retry-After")

                wait = (
                    int(retry_after)
                    if retry_after
                    else backoff ** attempt
                )

                logging.warning(
                    f"HTTP {response.status_code}. Retry {attempt+1}/{MAX_RETRIES} "
                    f"waiting {wait}s"
                )

                time.sleep(wait)
                continue

            logging.error(
                f"Companies House returned HTTP {response.status_code}"
            )

            return None

        except requests.RequestException as ex:

            wait = backoff ** attempt

            logging.warning(
                f"Network error ({ex}). Retry {attempt+1}/{MAX_RETRIES}"
            )

            time.sleep(wait)

    return None

def write_matches(client: bigquery.Client, matches: list):
    """Only real Companies House matches that are IT/tech companies land
    here — clean, filtered to your target industry, no noise."""
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{CH_TABLE}"
    schema = [
        bigquery.SchemaField("name_for_matching", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("company_number", "STRING"),
        bigquery.SchemaField("company_status", "STRING"),
        bigquery.SchemaField("company_type", "STRING"),
        bigquery.SchemaField("date_of_creation", "STRING"),
        bigquery.SchemaField("sic_codes", "STRING", mode="REPEATED"),
        bigquery.SchemaField("registered_office_address", "STRING"),
        bigquery.SchemaField("matched_at", "TIMESTAMP"),
    ]
    _write_json_rows(client, table_ref, matches, schema)


def write_attempts(client: bigquery.Client, attempts: list):
    """Every sponsor we tried (matched or not) is logged here, so re-runs
    never repeat an API call for the same name."""
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{ATTEMPTS_TABLE}"
    schema = [
        bigquery.SchemaField("name_for_matching", "STRING"),
        bigquery.SchemaField("was_matched", "BOOL"),
        bigquery.SchemaField("attempted_at", "TIMESTAMP"),
    ]
    _write_json_rows(client, table_ref, attempts, schema)


@functions_framework.http
def enrich_companies_house(request):
    if not CH_API_KEY:
        return {"status": "error", "message": "COMPANIES_HOUSE_API_KEY env var not set"}, 500

    client = bigquery.Client()

    try:
        sponsor_names = get_unattempted_sponsors(client, BATCH_SIZE)
    except Exception as e:
        # Likely companies_house_attempts doesn't exist yet on first run
        return {
            "status": "error",
            "message": f"Failed reading unattempted sponsors (attempts table may not exist yet): {e}",
        }, 500

    matches = []
    attempts = []
    matched_count = 0
    tech_count = 0
    not_found_count = 0

    now = datetime.now(timezone.utc).isoformat()

    for name in sponsor_names:
        logging.info(f"Processing sponsor: {name}")
        search_result = search_company(name)
        if search_result:
            logging.info(
                f"Matched: {search_result.get('company_number')}"
            )
        else:
            logging.info("No Companies House match")

        time.sleep(REQUEST_DELAY_SECONDS)

        if not search_result:
            attempts.append({"name_for_matching": name, "was_matched": False, "attempted_at": now})
            not_found_count += 1
            continue

        matched_count += 1
        company_number = search_result.get("company_number")

        profile = get_company_profile(company_number) if company_number else None
        time.sleep(REQUEST_DELAY_SECONDS)
        if profile:
            logging.info(
                f"SIC codes: {profile.get('sic_codes', [])}"
            )

        sic_codes = (profile or {}).get("sic_codes", [])

        if is_tech_company(sic_codes):
            logging.info("Tech company")
            address = (profile or {}).get("registered_office_address", {})
            address_str = ", ".join(
                str(v) for v in [
                    address.get("address_line_1"),
                    address.get("locality"),
                    address.get("postal_code"),
                ] if v
            )
        else:
            logging.info("Non-tech company")
            
            matches.append({
                "name_for_matching": name,
                "company_name": search_result.get("title"),
                "company_number": company_number,
                "company_status": search_result.get("company_status"),
                "company_type": (profile or {}).get("type"),
                "date_of_creation": search_result.get("date_of_creation"),
                "sic_codes": sic_codes,
                "registered_office_address": address_str,
                "matched_at": now,
            })
            tech_count += 1

        # Every sponsor we found on Companies House is logged as "attempted
        # and matched", regardless of whether it passed the tech filter —
        # this prevents re-querying non-tech companies on future runs.
        attempts.append({"name_for_matching": name, "was_matched": True, "attempted_at": now})

    if matches:
        write_matches(client, matches)
    if attempts:
        write_attempts(client, attempts)

    return {
        "status": "success",
        "processed": len(sponsor_names),
        "companies_house_matches": matched_count,
        "tech_companies_kept": tech_count,
        "not_found": not_found_count,
        "note": "raw_companies_house holds tech/IT matches only (filtered by SIC code); companies_house_attempts logs every name tried so re-runs never repeat work.",
    }, 200
