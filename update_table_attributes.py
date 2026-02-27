"""
Script: Update DynamoDB Table Attribute Values

Scans a DynamoDB table and updates specific attribute values based on a
provided mapping. Only updates records that actually contain the attribute.

Usage:
  python update_table_attributes.py                     # dry run (default)
  python update_table_attributes.py --apply             # apply changes
  python update_table_attributes.py --table MyTable     # specify table name
  python update_table_attributes.py --pk MyPartitionKey # specify partition key name
  python update_table_attributes.py --region us-east-1  # specify AWS region
"""

import argparse
import logging
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- Attribute Value Overrides ----------
# Keys = attribute names to look for in each item
# Values = what those attributes should be set to
ATTRIBUTE_OVERRIDES: Dict[str, str] = {
    # "WelcomeGuide_Q4_Yes": "Your custom label here",
    # "WelcomeGuide_Q4_No": "Another custom label",
}


def update_table(table_name: str, pk_name: str, dry_run: bool, region: str) -> Dict[str, Any]:
    """Scan the table and update matching attributes."""
    if not ATTRIBUTE_OVERRIDES:
        logger.warning("ATTRIBUTE_OVERRIDES is empty. Nothing to update.")
        return {"scanned": 0, "updated": 0, "errors": 0}

    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)
    scanned = 0
    updated = 0
    errors = 0

    logger.info("Starting scan: table=%s, region=%s, overrides=%d, dry_run=%s",
                table_name, region, len(ATTRIBUTE_OVERRIDES), dry_run)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update DynamoDB table attribute values in bulk.")
    parser.add_argument("--table", default="ConnectViewData", help="DynamoDB table name (default: ConnectViewData)")
    parser.add_argument("--pk", default="InitialContactId", help="Partition key name (default: InitialContactId)")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag, runs in dry-run mode.")
    args = parser.parse_args()

    dry_run = not args.apply
    if dry_run:
        print("Running in DRY RUN mode. Use --apply to make changes.\n")

    result = update_table(args.table, args.pk, dry_run, args.region)
    print(f"\nResult: {result}")
