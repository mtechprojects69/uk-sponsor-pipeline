"""
Cloud Function: ingest_sponsors
...
"""

import os
import re
import io
from datetime import datetime, timezone

import requests
import functions_framework
from google.cloud import storage
from google.cloud import bigquery

GOV_UK_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
BUCKET_NAME = os.environ.get("SPONSORS_BUCKET", "REPLACE_WITH_YOUR_BUCKET_NAME")
BQ_DATASET = os.environ.get("BQ_DATASET", "uk_sponsor_pipeline")
BQ_TABLE = os.environ.get("BQ_TABLE", "raw_sponsors")
BQ_PROJECT = os.environ.get("GCP_PROJECT")

CSV_LINK_PATTERN = re.compile(
    r'https://assets\.publishing\.service\.gov\.uk/media/[^\s"\']+\.csv'
)


def find_current_csv_url() -> str:
    resp = requests.get(GOV_UK_PAGE, timeout=30, headers={"User-Agent": "uk-sponsor-pipeline/1.0"})
    resp.raise_for_status()
    match = CSV_LINK_PATTERN.search(resp.text)
    if not match:
        raise RuntimeError("Could not find a CSV link on the GOV.UK page — page layout may have changed.")
    return match.group(0)


def download_csv(url: str) -> bytes:
    resp = requests.get(url, timeout=120, headers={"User-Agent": "uk-sponsor-pipeline/1.0"})
    resp.raise_for_status()
    return resp.content


def upload_to_gcs(content: bytes, filename: str) -> str:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob_path = f"raw/sponsors/{filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="text/csv")
    return f"gs://{BUCKET_NAME}/{blob_path}"


def load_to_bigquery(gcs_uri: str):
    client = bigquery.Client()
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    return table.num_rows


@functions_framework.http
def ingest_sponsors(request):
    try:
        csv_url = find_current_csv_url()
        content = download_csv(csv_url)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"sponsors_{today}.csv"
        gcs_uri = upload_to_gcs(content, filename)
        row_count = load_to_bigquery(gcs_uri)
        result = {
            "status": "success",
            "source_csv_url": csv_url,
            "gcs_uri": gcs_uri,
            "bigquery_table": f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
            "rows_loaded": row_count,
        }
        return result, 200
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
