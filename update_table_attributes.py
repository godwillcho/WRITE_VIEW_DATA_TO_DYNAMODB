"""
Lambda Function: Update DynamoDB Table Attribute Values

Scans a DynamoDB table and updates specific attribute values based on a
provided mapping. Only updates records that actually contain the attribute.

Event format:
{
  "TableName": "ConnectViewData",
  "DryRun": false
}

Set DryRun to true to see what would be updated without making changes.
"""

import os
import logging
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

# ---------- Logging ----------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_dynamodb = boto3.resource("dynamodb")

# ---------- Attribute Value Overrides ----------
# Keys = attribute names to look for in each item
# Values = what those attributes should be set to
ATTRIBUTE_OVERRIDES: Dict[str, str] = {
    # "WelcomeGuide_Q4_Yes": "Your custom label here",
    # "WelcomeGuide_Q4_No": "Another custom label",
}


def lambda_handler(event, context):
    """Scan the table and update matching attributes."""
    table_name = event.get("TableName") or os.getenv("DDB_TABLE_NAME")
    dry_run = event.get("DryRun", False)
    pk_name = event.get("PartitionKeyName") or os.getenv("DDB_PK_NAME", "InitialContactId")

    if not table_name:
        logger.error("No TableName provided in event or DDB_TABLE_NAME env var.")
        return {"error": "TableName is required"}

    if not ATTRIBUTE_OVERRIDES:
        logger.warning("ATTRIBUTE_OVERRIDES is empty. Nothing to update.")
        return {"scanned": 0, "updated": 0, "errors": 0}

    table = _dynamodb.Table(table_name)
    scanned = 0
    updated = 0
    errors = 0

    logger.info("Starting scan: table=%s, overrides=%d, dry_run=%s",
                table_name, len(ATTRIBUTE_OVERRIDES), dry_run)

    scan_kwargs: Dict[str, Any] = {}
    while True:
        try:
            response = table.scan(**scan_kwargs)
        except ClientError as e:
            logger.error("Scan failed: %s: %s", type(e).__name__, str(e))
            return {"scanned": scanned, "updated": updated, "errors": errors, "error": str(e)}

        items = response.get("Items", [])
        scanned += len(items)

        for item in items:
            pk_value = item.get(pk_name)
            if not pk_value:
                continue

            # Find which overrides apply to this item
            updates: Dict[str, str] = {}
            for attr_name, new_value in ATTRIBUTE_OVERRIDES.items():
                if attr_name in item and item[attr_name] != new_value:
                    updates[attr_name] = new_value

            if not updates:
                continue

            if dry_run:
                logger.info("[DRY RUN] Would update pk=%s: %s",
                            pk_value, {k: f"{item[k]} -> {v}" for k, v in updates.items()})
                updated += 1
                continue

            # Build the update expression
            expr_parts = []
            expr_names: Dict[str, str] = {}
            expr_values: Dict[str, str] = {}
            for i, (attr_name, new_value) in enumerate(updates.items()):
                placeholder_name = f"#a{i}"
                placeholder_value = f":v{i}"
                expr_parts.append(f"{placeholder_name} = {placeholder_value}")
                expr_names[placeholder_name] = attr_name
                expr_values[placeholder_value] = new_value

            update_expr = "SET " + ", ".join(expr_parts)

            try:
                table.update_item(
                    Key={pk_name: pk_value},
                    UpdateExpression=update_expr,
                    ExpressionAttributeNames=expr_names,
                    ExpressionAttributeValues=expr_values,
                )
                updated += 1
                logger.info("Updated pk=%s: %s", pk_value, list(updates.keys()))
            except ClientError as e:
                errors += 1
                logger.error("Update failed for pk=%s: %s: %s",
                             pk_value, type(e).__name__, str(e))

        # Pagination
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    logger.info("Complete: scanned=%d, updated=%d, errors=%d, dry_run=%s",
                scanned, updated, errors, dry_run)
    return {"scanned": scanned, "updated": updated, "errors": errors, "dry_run": dry_run}
