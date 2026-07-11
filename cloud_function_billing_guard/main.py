"""
Cloud Function: billing_guard

Purpose: a hard financial circuit breaker.

Budget alerts in GCP only SEND EMAILS — they don't stop anything from running.
This function closes that gap: it subscribes to the Pub/Sub topic your budget
publishes to, and if actual spend has reached (or exceeded) the budget amount,
it disables billing on the project entirely. Once billing is disabled, GCP
stops all billable resources (Cloud Functions, BigQuery jobs, etc. can no
longer run) — so you cannot be charged beyond this point.

Trigger: Pub/Sub (the topic your Billing Budget is configured to publish to).

IMPORTANT: This is a one-way, drastic action for safety. Re-enabling billing
afterwards must be done manually by you in the Console.
"""

import base64
import json
import os

import functions_framework
from google.cloud import billing_v1

# gen2 Cloud Functions don't set GCP_PROJECT automatically (that was gen1-only),
# so check GOOGLE_CLOUD_PROJECT first.
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
PROJECT_NAME = f"projects/{PROJECT_ID}"


def _is_billing_enabled(billing_client: billing_v1.CloudBillingClient) -> bool:
    project_billing_info = billing_client.get_project_billing_info(name=PROJECT_NAME)
    return project_billing_info.billing_enabled


def _disable_billing(billing_client: billing_v1.CloudBillingClient):
    project_billing_info = billing_v1.ProjectBillingInfo(billing_account_name="")
    billing_client.update_project_billing_info(
        name=PROJECT_NAME,
        project_billing_info=project_billing_info,
    )


@functions_framework.cloud_event
def billing_guard(cloud_event):
    """gen2 Pub/Sub triggers deliver a CloudEvent, not a plain dict —
    the actual Pub/Sub message lives at cloud_event.data["message"]."""

    pubsub_message = cloud_event.data["message"]
    pubsub_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))

    cost_amount = pubsub_data.get("costAmount", 0)
    budget_amount = pubsub_data.get("budgetAmount", 0)

    print(f"Budget check for {PROJECT_ID}: spent {cost_amount}, budget {budget_amount}")

    if cost_amount <= budget_amount:
        print("Spend is within budget — no action taken.")
        return

    billing_client = billing_v1.CloudBillingClient()

    if not _is_billing_enabled(billing_client):
        print("Billing is already disabled — nothing to do.")
        return

    print(f"Spend ({cost_amount}) has reached/exceeded budget ({budget_amount}). Disabling billing now.")
    _disable_billing(billing_client)
    print("Billing disabled. All billable resources will stop running.")
