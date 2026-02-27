"""
Lambda Function: Extract Questions/Labels from Amazon Connect Views

Receives a list of View ARNs, calls describe_view for each, recursively
extracts all labels/questions from the View template JSON, and writes
each View's data as an item to DynamoDB.

Event format:
{
  "InstanceId": "9b50ddcc-8510-441e-a9c8-96e9e9281105",
  "ViewArns": [
    "arn:aws:connect:us-east-1:123456789012:instance/xxx/view/yyy",
    ...
  ]
}

DynamoDB item per Name+Label pair:
- PK = Name (component name, e.g. "REDE_Q2")
- SK = Label (display text, e.g. "Yes - to Transferred to CCH_AA")
- ViewId, ViewArn, ViewName, ViewStatus, ViewDescription
- CreatedAt (human-readable timestamp in configured timezone)
"""

import os
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

# ---------- Logging ----------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ---------- Config via env ----------
_DDB_TABLE = os.getenv("DDB_TABLE_NAME")
_DDB_PK = os.getenv("DDB_PK_NAME", "Name")
_DDB_SK = os.getenv("DDB_SK_NAME", "Label")
try:
    _TZ_OFFSET = int(os.getenv("TZ_OFFSET_HOURS", "-5"))
except ValueError:
    _TZ_OFFSET = -5
_TZ_LABEL = os.getenv("TZ_LABEL", "EST")

_dynamodb = boto3.resource("dynamodb") if _DDB_TABLE else None
_connect_client = boto3.client("connect")

# ---------- Label Overrides ----------
# Map specific Name values to a custom Label.
# If a Name extracted from a View template matches a key here,
# the override Label will be used instead of the one from the template.
LABEL_OVERRIDES: Dict[str, str] = {
    # "WelcomeGuide_Q4_Yes": "Custom label here",
}


def extract_name_label_pairs(data: Any) -> List[Dict[str, str]]:
    """Recursively walk the View template and collect Name+Label pairs.
    Only collects from dicts that have their own Name key (questions only)."""
    pairs: List[Dict[str, str]] = []
    if isinstance(data, dict):
        name = data.get("Name") or data.get("name")
        label = None
        for key in ("Label", "label"):
            if key in data and isinstance(data[key], str):
                label = data[key]
                break
        if name and label:
            pairs.append({"Name": name, "Label": label})
        for value in data.values():
            pairs.extend(extract_name_label_pairs(value))
    elif isinstance(data, list):
        for item in data:
            pairs.extend(extract_name_label_pairs(item))
    return pairs


def _parse_view_arn(view_arn: str) -> Dict[str, str]:
    """Extract InstanceId and ViewId from a View ARN."""
    # ARN format: arn:aws:connect:region:account:instance/INSTANCE_ID/view/VIEW_ID
    parts = view_arn.split("/")
    instance_id = parts[1] if len(parts) >= 2 else ""
    view_id = parts[3] if len(parts) >= 4 else ""
    return {"instance_id": instance_id, "view_id": view_id}


def _write_to_dynamodb(item: Dict[str, Any]) -> None:
    """Write a single item to DynamoDB."""
    if not _dynamodb:
        logger.info("DynamoDB not configured: skipping write.")
        return

    try:
        table = _dynamodb.Table(_DDB_TABLE)
        table.put_item(Item=item)
        logger.info("DynamoDB write success: table=%s pk=%s",
                    _DDB_TABLE, item.get(_DDB_PK, "unknown"))
    except ClientError as e:
        logger.error("DynamoDB ClientError: %s: %s", type(e).__name__, str(e))
    except Exception as e:
        logger.error("DynamoDB write failed: %s: %s", type(e).__name__, str(e))


def _list_view_arns(instance_id: str) -> List[str]:
    """Call connect:ListViews and return all View ARNs."""
    arns: List[str] = []
    next_token = None
    while True:
        kwargs: Dict[str, Any] = {"InstanceId": instance_id}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = _connect_client.list_views(**kwargs)
        for vs in resp.get("ViewsSummaryList", []):
            arn = vs.get("Arn")
            if arn:
                arns.append(arn)
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return arns


def lambda_handler(event, context):
    """Process a list of View ARNs: extract questions and write to DynamoDB."""
    view_arns: List[str] = event.get("ViewArns", [])
    instance_id_override: str = event.get("InstanceId", "")

    # Auto-discover views when ViewArns is not provided
    if not view_arns and instance_id_override:
        logger.info("No ViewArns provided; discovering views for instance %s.", instance_id_override)
        view_arns = _list_view_arns(instance_id_override)
        logger.info("Discovered %d view(s).", len(view_arns))

    if not view_arns:
        logger.warning("No ViewArns provided or discovered.")
        return {"processed": 0, "errors": 0}

    logger.info("Processing %d view(s).", len(view_arns))

    tz = timezone(timedelta(hours=_TZ_OFFSET))
    processed = 0
    errors = 0

    for view_arn in view_arns:
        try:
            parsed = _parse_view_arn(view_arn)
            instance_id = instance_id_override or parsed["instance_id"]
            view_id = parsed["view_id"]

            if not instance_id or not view_id:
                logger.error("Could not parse InstanceId/ViewId from ARN: %s", view_arn)
                errors += 1
                continue

            # Describe the View
            response = _connect_client.describe_view(
                InstanceId=instance_id,
                ViewId=view_id,
            )
            view = response.get("View", {})

            # Parse the template JSON and extract questions
            template_str = view.get("Content", {}).get("Template", "{}")
            if isinstance(template_str, str):
                view_content = json.loads(template_str)
            else:
                view_content = template_str

            pairs = extract_name_label_pairs(view_content)
            created_at = datetime.now(tz).strftime(f"%B %d, %Y %I:%M:%S %p {_TZ_LABEL}")

            view_name = view.get("Name")
            view_status = view.get("Status")
            view_description = view.get("Description")

            for pair in pairs:
                label = LABEL_OVERRIDES.get(pair["Name"], pair["Label"])
                item: Dict[str, Any] = {
                    _DDB_PK: pair["Name"],
                    _DDB_SK: label,
                    "ViewId": view_id,
                    "ViewArn": view_arn,
                    "CreatedAt": created_at,
                }
                if view_name:
                    item["ViewName"] = view_name
                if view_status:
                    item["ViewStatus"] = view_status
                if view_description:
                    item["ViewDescription"] = view_description

                _write_to_dynamodb(item)

            processed += 1
            logger.info("Wrote %d pair(s) for view %s.", len(pairs), view_id)

        except ClientError as e:
            errors += 1
            logger.error("describe_view failed for %s: %s: %s", view_arn, type(e).__name__, str(e))
        except Exception as e:
            errors += 1
            logger.error("Error processing view %s: %s: %s", view_arn, type(e).__name__, str(e))

    logger.info("Complete: processed=%d, errors=%d", processed, errors)
    return {"processed": processed, "errors": errors}
