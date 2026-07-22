"""
Data Freshness Checker Lambda

Triggered weekly by EventBridge to verify key pipeline tables have recent data.
Sends SNS alert if any table is staler than its configured threshold.

Environment variables:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    SNS_TOPIC_ARN - ARN for pipeline alert notifications
"""

import logging
import os
from datetime import datetime
import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")

FRESHNESS_CHECKS = [
    {
        "table": "gisapdata.bt_footfall_tfl_hex_3hourly",
        "date_column": "count_date",
        "max_age_days": 10,
        "label": "BT Hex 3-hourly",
    },
    {
        "table": "gisapdata.econ_busyness_bt_3hourly_counts",
        "date_column": "count_date",
        "max_age_days": 10,
        "label": "BT 3-hourly Counts (combined)",
    },
    {
        "table": "gisapdata.econ_busyness_mcard_txn",
        "date_column": "week_start",
        "max_age_days": 45,
        "label": "Mastercard Weekly Txn (combined)",
    },
    {
        "table": "gisapdata.econ_busyness_bt_daily_agg_cust_raw",
        "date_column": "count_date",
        "max_age_days": 10,
        "label": "BT Daily Aggregate",
    },
]


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ.get("PG_PORT", "5432"),
        database=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def check_freshness(conn, check):
    """Return (max_date, is_stale, age_days) for a single table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX({check['date_column']}) "
            f"FROM {check['table']}"
        )
        row = cur.fetchone()
        max_date = row[0] if row and row[0] else None

    if max_date is None:
        return None, True, None

    today = datetime.utcnow().date()
    age_days = (today - max_date).days
    is_stale = age_days > check["max_age_days"]
    return max_date, is_stale, age_days


def handler(event, context):
    conn = get_pg_connection()
    results = []
    stale_tables = []

    try:
        for check in FRESHNESS_CHECKS:
            max_date, is_stale, age_days = check_freshness(conn, check)
            status = "STALE" if is_stale else "OK"
            result = {
                "label": check["label"],
                "table": check["table"],
                "max_date": str(max_date),
                "age_days": age_days,
                "threshold_days": check["max_age_days"],
                "status": status,
            }
            results.append(result)
            logger.info(
                f"{check['label']}: {status} "
                f"(max_date={max_date}, age={age_days}d)"
            )

            if is_stale:
                stale_tables.append(result)
    finally:
        conn.close()

    if stale_tables:
        subject = f"HSDS Data Freshness Alert: {len(stale_tables)} stale table(s)"
        body_lines = ["The following HSDS tables have stale data:\n"]
        for t in stale_tables:
            body_lines.append(
                f"  - {t['label']}: last data {t['max_date']} "
                f"({t['age_days']} days old, threshold {t['threshold_days']}d)"
            )
        body_lines.append(f"\nChecked at: {datetime.utcnow().isoformat()}Z")
        message = "\n".join(body_lines)

        topic_arn = os.environ.get("SNS_TOPIC_ARN")
        if topic_arn:
            sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject[:100],
                Message=message,
            )
            logger.info(f"Sent alert: {subject}")

    return {"statusCode": 200, "results": results}
