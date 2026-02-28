"""
Lambda Function: Process Amazon Connect TASK Contact Flow Events

Invoked from an Amazon Connect contact flow for TASK channel events.
Extracts the Case ID from the taskRef URL, calls the Cases API
(connectcases:GetCase) to retrieve all case fields, sends an SNS
notification with the data as JSON, and returns the case fields
as flat key-value pairs.

Environment variables:
  CASES_DOMAIN_ID  - Amazon Connect Cases domain ID
  SNS_TOPIC_ARN    - ARN of the SNS topic for email notifications
  LOG_LEVEL        - Logging level (DEBUG | INFO | WARNING | ERROR). Default: INFO
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

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
_CASES_DOMAIN_ID = os.getenv("CASES_DOMAIN_ID")
_SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

# ---------- Boto3 clients (module-level for connection reuse) ----------
_cases_client = boto3.client("connectcases")
_sns_client = boto3.client("sns")

# ---------- Field name cache ----------
# Maps field ID -> human-readable field name (populated by _list_all_field_ids)
_field_name_cache: Dict[str, str] = {}


def _list_all_field_ids(domain_id: str) -> List[Dict[str, str]]:
    """Call connectcases:ListFields to discover all field IDs for the domain.

    Returns a list of {"id": field_id} dicts ready to pass to get_case.
    Also populates _field_name_cache with field_id -> field_name mappings
    so the response can include human-readable names.

    The get_case API accepts max 220 fields per call.
    """
    field_ids: List[Dict[str, str]] = []
    next_token: Optional[str] = None

    while True:
        kwargs: Dict[str, Any] = {"domainId": domain_id}
        if next_token:
            kwargs["nextToken"] = next_token

        try:
            resp = _cases_client.list_fields(**kwargs)
        except ClientError as e:
            logger.error("connectcases:ListFields failed: %s: %s", type(e).__name__, str(e))
            raise

        for field in resp.get("fields", []):
            fid = field.get("fieldId", "")
            fname = field.get("name", "")
            if fid:
                field_ids.append({"id": fid})
                _field_name_cache[fid] = fname

        next_token = resp.get("nextToken")
        if not next_token:
            break

    logger.info("Discovered %d field(s) for domain %s.", len(field_ids), domain_id)
    return field_ids


def _extract_case_id_from_task_ref(event: Dict[str, Any]) -> Optional[str]:
    """Extract the Amazon Connect Case ID from the taskRef URL in the event.

    The Case ID is the last path segment of the taskRef URL:
    https://.../cases/agent-app/case-detail/de4d7f2d-592d-3b68-908d-d09e5dc93471

    Returns None if taskRef is missing or the URL cannot be parsed.
    """
    try:
        references = (
            event.get("Details", {})
            .get("ContactData", {})
            .get("References", {})
        )
        task_ref = references.get("taskRef", {})
        task_ref_url = task_ref.get("Value", "")

        if not task_ref_url:
            logger.warning("taskRef URL is empty or missing in event References.")
            return None

        parsed = urlparse(task_ref_url)
        path_segments = [s for s in parsed.path.split("/") if s]

        if not path_segments:
            logger.error("Could not extract Case ID: taskRef URL path is empty: %s", task_ref_url)
            return None

        case_id = path_segments[-1]
        logger.info("Extracted Case ID: %s from taskRef URL.", case_id)
        return case_id

    except Exception as e:
        logger.error("Failed to extract Case ID from taskRef: %s: %s", type(e).__name__, str(e))
        return None


def process_task_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process a TASK channel event: extract Case ID and retrieve case fields.

    Extracts the Case ID from the taskRef URL in the event References,
    then calls connectcases:GetCase to retrieve the case field values.

    Returns a dict with:
      - channel: "TASK"
      - case_id: the extracted Case ID
      - contact_id: the ContactId from the event
      - task_name: the Name from ContactData
      - fields: dict of {field_id: field_value} from the Cases API response
    """
    if not _CASES_DOMAIN_ID:
        raise ValueError("CASES_DOMAIN_ID environment variable is not set. Cannot process TASK event.")

    contact_data = event.get("Details", {}).get("ContactData", {})
    contact_id = contact_data.get("ContactId", "")
    task_name = contact_data.get("Name", "")

    case_id = _extract_case_id_from_task_ref(event)
    if not case_id:
        raise ValueError(f"Could not extract Case ID from event for contact {contact_id}.")

    logger.info("Processing TASK event: contact_id=%s, case_id=%s, task_name=%s",
                contact_id, case_id, task_name)

    # Step 1: Discover all fields for this domain
    all_field_ids = _list_all_field_ids(_CASES_DOMAIN_ID)
    if not all_field_ids:
        raise ValueError(f"No fields found for domain {_CASES_DOMAIN_ID}.")

    # Step 2: Call get_case with all discovered fields.
    # The API accepts max 220 fields per call, so batch if needed.
    raw_fields: List[Dict[str, Any]] = []
    batch_size = 220
    for i in range(0, len(all_field_ids), batch_size):
        batch = all_field_ids[i:i + batch_size]
        try:
            response = _cases_client.get_case(
                domainId=_CASES_DOMAIN_ID,
                caseId=case_id,
                fields=batch,
            )
            raw_fields.extend(response.get("fields", []))
        except ClientError as e:
            logger.error("connectcases:GetCase failed for case %s: %s: %s",
                         case_id, type(e).__name__, str(e))
            raise

    # Also capture the templateId from the last response
    template_id = response.get("templateId", "") if raw_fields else ""

    # Step 3: Parse field values using human-readable field names as keys.
    # The value is a tagged union: stringValue, doubleValue, booleanValue,
    # emptyValue, or userArnValue.
    fields: Dict[str, Any] = {}
    for field in raw_fields:
        field_id = field.get("id", "")
        field_name = _field_name_cache.get(field_id, field_id)
        value_obj = field.get("value", {})
        if "stringValue" in value_obj:
            fields[field_name] = value_obj["stringValue"]
        elif "doubleValue" in value_obj:
            fields[field_name] = value_obj["doubleValue"]
        elif "booleanValue" in value_obj:
            fields[field_name] = value_obj["booleanValue"]
        elif "userArnValue" in value_obj:
            fields[field_name] = value_obj["userArnValue"]
        elif "emptyValue" in value_obj:
            fields[field_name] = None
        else:
            fields[field_name] = str(value_obj)
            logger.warning("Unknown value type for field %s (%s): %s", field_name, field_id, value_obj)

    # Include any discovered fields not returned by get_case as None
    for _, fname in _field_name_cache.items():
        if fname not in fields:
            fields[fname] = None

    result = {
        "channel": "TASK",
        "contact_id": contact_id,
        "case_id": case_id,
        "task_name": task_name,
        "template_id": template_id,
        "fields": fields,
    }

    logger.info("TASK processing complete: case_id=%s, fields_count=%d (of %d discovered)",
                case_id, len(fields), len(all_field_ids))
    return result


def send_sns_notification(result: Dict[str, str], contact_id: str, sns_topic_arn: str) -> Optional[str]:
    """Publish a notification to SNS with the flat result data as JSON.

    Args:
        result: The flat key-value dict (same structure returned to Connect).
        contact_id: The ContactId for the SNS subject line.
        sns_topic_arn: ARN of the SNS topic to publish to.

    Returns:
        The SNS MessageId on success, or None on failure.
    """
    if not sns_topic_arn:
        logger.warning("SNS topic ARN not configured; skipping notification.")
        return None

    subject = f"Amazon Connect TASK - Contact {contact_id}"
    # SNS email subject max is 100 characters
    if len(subject) > 100:
        subject = subject[:97] + "..."

    message_body = json.dumps(result, indent=2, default=str)

    try:
        response = _sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=message_body,
        )
        message_id = response.get("MessageId", "")
        logger.info("SNS notification sent: message_id=%s, topic=%s, contact=%s",
                     message_id, sns_topic_arn, contact_id)
        return message_id
    except ClientError as e:
        logger.error("SNS publish failed: %s: %s", type(e).__name__, str(e))
        return None
    except Exception as e:
        logger.error("SNS publish unexpected error: %s: %s", type(e).__name__, str(e))
        return None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main entry point: process TASK event, notify via SNS, return flat data.

    Invoked from an Amazon Connect contact flow. Extracts case fields,
    publishes an SNS notification, and returns all case fields plus the
    channel and SNS message ID as flat key-value pairs.
    """
    try:
        contact_data = event.get("Details", {}).get("ContactData", {})
        contact_id = contact_data.get("ContactId", "unknown")

        logger.info("Lambda invoked: contact_id=%s", contact_id)

        data = process_task_event(event)

        # Build flat result from case fields (field names as keys, None for empty)
        result: Dict[str, Any] = {"channel": "TASK"}
        for key, value in data.get("fields", {}).items():
            result[key] = value

        # Send the flat result as the SNS message, then add sns_message_id
        message_id = send_sns_notification(result, contact_id, _SNS_TOPIC_ARN)
        result["sns_message_id"] = message_id or "not_sent"

        logger.info("Lambda complete: contact_id=%s, fields=%d, sns=%s",
                     contact_id, len(data.get("fields", {})),
                     "sent" if message_id else "skipped")
        return result

    except ValueError as e:
        logger.error("Validation error: %s: %s", type(e).__name__, str(e))
        return {
            "channel": "TASK",
            "error": str(e),
        }
    except Exception as e:
        logger.error("Fatal error: %s: %s", type(e).__name__, str(e))
        raise
