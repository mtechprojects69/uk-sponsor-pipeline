import logging
from io import BytesIO
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT_ID = "uk-sponsor-pipeline"
DATASET = "uk_sponsor_pipeline_reference"
TABLE = "ref_sic_codes"

SOURCE_URL = (
    "https://www.ons.gov.uk/file?uri="
    "/methodology/classificationsandstandards/"
    "ukstandardindustrialclassificationofeconomicactivities/"
    "uksic2007/publisheduksicsummaryofstructureworksheet.xlsx"
)


def load_sic_reference(request):
    logging.info("===== STARTING SIC REFERENCE LOAD =====")

    try:
        # ------------------------------------------------------------------
        # Download
        # ------------------------------------------------------------------
        logging.info("Downloading spreadsheet...")

        response = requests.get(SOURCE_URL, timeout=120)
        response.raise_for_status()

        logging.info("Download completed.")

        # ------------------------------------------------------------------
        # Read Excel
        # ------------------------------------------------------------------
        logging.info("Reading Excel...")

        df = pd.read_excel(
            BytesIO(response.content),
            sheet_name="reworked structure",
            engine="openpyxl",
        )

        logging.info("Rows read: %s", len(df))

        # ------------------------------------------------------------------
        # Rename columns
        # ------------------------------------------------------------------
        df = df.rename(
            columns={
                "Description": "description",
                "SECTION": "section",
                "Division": "division",
                "Group": "group_code",
                "Class": "class_code",
                "Sub Class": "subclass_code",
                "Most disaggregated level": "sic_code",
                "Level headings": "level",
            }
        )

        # ------------------------------------------------------------------
        # Keep only required columns
        # ------------------------------------------------------------------
        columns = [
            "sic_code",
            "description",
            "section",
            "division",
            "group_code",
            "class_code",
            "subclass_code",
            "level",
        ]

        df = df[columns].copy()

        # ------------------------------------------------------------------
        # Convert everything to string
        # ------------------------------------------------------------------
        for col in columns:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.strip()
            )

        df["load_timestamp"] = datetime.now(timezone.utc).isoformat()

        logging.info("Dataframe prepared.")

        # ------------------------------------------------------------------
        # CSV in memory
        # ------------------------------------------------------------------
        csv_buffer = BytesIO()

        df.to_csv(
            csv_buffer,
            index=False,
            encoding="utf-8",
        )

        csv_buffer.seek(0)

        # ------------------------------------------------------------------
        # BigQuery
        # ------------------------------------------------------------------
        client = bigquery.Client(project=PROJECT_ID)

        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema=[
                bigquery.SchemaField("sic_code", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("section", "STRING"),
                bigquery.SchemaField("division", "STRING"),
                bigquery.SchemaField("group_code", "STRING"),
                bigquery.SchemaField("class_code", "STRING"),
                bigquery.SchemaField("subclass_code", "STRING"),
                bigquery.SchemaField("level", "STRING"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
            ],
        )

        logging.info("Loading into BigQuery...")

        job = client.load_table_from_file(
            csv_buffer,
            table_id,
            job_config=job_config,
        )

        job.result()

        table = client.get_table(table_id)

        logging.info(
            "Finished. %s rows loaded.",
            table.num_rows,
        )

        return (
            f"SUCCESS - Loaded {table.num_rows} rows into {table_id}",
            200,
        )

    except Exception as e:
        logging.exception("Load failed")
        return (str(e), 500)
