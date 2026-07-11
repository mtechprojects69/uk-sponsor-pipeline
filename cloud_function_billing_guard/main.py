"""
Cloud Function: billing_guard
Disables billing on the project automatically if spend reaches the budget.
"""

import base64
import json
import os

from google.cloud import billing_v1

PROJECT_ID = os.environ.get("GCP_PROJECT")
PROJECT_NAME = f"projects/{PROJECT_ID}"


def _is_billing_enabled(billing_client):
    info = billing_client.get_project_billing_info(name=PROJECT_NAME)
    return info.billing_enabled


def _disable_billing(billing_client):
    project_billing_info = billing_v1.ProjectBillingInfo(billing_account_name="")
    billing_client.update_project_billing_info(
        name=PROJECT_NAME,
        project_billing_info=project_billing_info,
    )


def billing_guard(event, context):
    pubsub_data = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
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
    print("Billing disabled.")
