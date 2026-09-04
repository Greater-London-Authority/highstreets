"""
BT Pipeline Auto-Scheduler Lambda

Triggered by EventBridge Scheduler every Thursday at 08:00 UTC.
Checks if the previous week's BT data has already been processed
by querying MAX(count_date) in PostgreSQL. If not, triggers the
BT Step Function with the correct date range.

Environment variables (set in Lambda configuration):
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    BT_STEP_FUNCTION_ARN  - ARN of the BT E2E Step Function
    SNS_TOPIC_ARN         - ARN for pipeline notifications (optional)

IAM permissions needed:
    states:StartExecution on BT Step Function ARN
    sns:Publish on SNS topic ARN (if notifications enabled)
    VPC access for RDS connectivity (or RDS Proxy)
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sfn_client = boto3.client("stepfunctions")
sns_client = boto3.client("sns")


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ.get("PG_PORT", "5432"),
        database=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def get_max_count_date():
    """Query the latest processed date from the BT hex table."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(count_date) FROM "
                "gisapdata.bt_footfall_tfl_hex_3hourly"
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    finally:
        conn.close()


def compute_week_boundaries(max_date=None):
    """Compute the next unprocessed week's Monday and Sunday.

    If max_date is provided, the next week starts the day after max_date's
    Monday. This prevents skipping weeks when the scheduler runs late.
    If max_date is None (empty table), falls back to previous week from today.
    """
    today = datetime.utcnow().date()

    if max_date:
        next_start = max_date + timedelta(days=1)
        days_since_monday = next_start.weekday()
        start_monday = next_start - timedelta(days=days_since_monday)
        end_sunday = start_monday + timedelta(days=6)

        if end_sunday >= today:
            return None, None

        return start_monday, end_sunday

    days_since_monday = today.weekday()
    this_monday = today - timedelta(days=days_since_monday)
    prev_monday = this_monday - timedelta(days=7)
    prev_sunday = this_monday - timedelta(days=1)
    return prev_monday, prev_sunday


def send_notification(subject, message):
    """Send SNS notification if topic ARN is configured."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        logger.info("No SNS_TOPIC_ARN configured, skipping notification")
        return
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=message,
    )


def handler(event, context):
    """Lambda entry point."""
    max_date = get_max_count_date()
    logger.info(f"Latest processed date in PG: {max_date}")

    start_date, end_date = compute_week_boundaries(max_date)

    if start_date is None:
        msg = (
            f"BT data processed up to {max_date}. "
            f"Next week ({max_date + timedelta(days=1)}+) has not ended yet. Skipping."
        )
        logger.info(msg)
        send_notification("HSDS BT Scheduler: Skipped (week not complete)", msg)
        return {"statusCode": 200, "action": "skipped", "max_date": str(max_date)}

    logger.info(
        f"Target week: {start_date.isoformat()} to {end_date.isoformat()}"
    )

    step_function_arn = os.environ["BT_STEP_FUNCTION_ARN"]
    sf_input = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }
    execution_name = f"auto-{start_date.isoformat()}-{end_date.isoformat()}"

    logger.info(f"Triggering Step Function: {step_function_arn}")
    logger.info(f"Input: {json.dumps(sf_input)}")

    response = sfn_client.start_execution(
        stateMachineArn=step_function_arn,
        name=execution_name,
        input=json.dumps(sf_input),
    )

    msg = (
        f"BT pipeline triggered for {start_date} to {end_date}.\n"
        f"Execution ARN: {response['executionArn']}\n"
        f"Previous max_date in PG: {max_date}"
    )
    logger.info(msg)
    send_notification("HSDS BT Scheduler: Pipeline triggered", msg)

    return {
        "statusCode": 200,
        "action": "triggered",
        "executionArn": response["executionArn"],
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }
