"""
Lambda Function: Process Amazon Connect Contact Flow Events (TASK / CHAT)

Invoked from an Amazon Connect contact flow. Routes based on the Channel field:
- TASK: Extracts the Case ID from the taskRef URL, calls the Cases API
        (connectcases:GetCase) to retrieve case fields, and sends an SNS
        notification with all case field data as JSON.
- CHAT: Extracts contact attributes from the event and sends an SNS
        notification with all attributes as JSON.

Environment variables:
  CASES_DOMAIN_ID  - Amazon Connect Cases domain ID (required for TASK processing)
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

# ---------- Case fields to request from the Cases API ----------
# Built-in fields. Add custom field IDs to CUSTOM_CASE_FIELD_IDS below.
DEFAULT_CASE_FIELD_IDS = [
    "status",
    "title",
    "assigned_queue",
    "assigned_user",
    "case_reason",
    "last_closed_datetime",
    "created_datetime",
    "last_updated_datetime",
    "reference_number",
    "summary",
]

# Custom case field IDs (add your own field IDs here).
# Find available field IDs by running:
#   python enable_case_event_streams.py --list-fields --instance-id ... --domain-id ...
CUSTOM_CASE_FIELD_IDS: List[str] = [
    # "my_custom_field_1",
    # "my_custom_field_2",
]

# Combined list sent to the Cases API
CASE_FIELD_IDS: List[Dict[str, str]] = [
    {"id": f} for f in DEFAULT_CASE_FIELD_IDS + CUSTOM_CASE_FIELD_IDS
]


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

    # Call the Cases API to get case field values
    try:
        response = _cases_client.get_case(
            domainId=_CASES_DOMAIN_ID,
            caseId=case_id,
            fields=CASE_FIELD_IDS,
        )
    except ClientError as e:
        logger.error("connectcases:GetCase failed for case %s: %s: %s",
                     case_id, type(e).__name__, str(e))
        raise

    # Parse the response: fields come back as a list of {id, value} dicts.
    # The value is a tagged union with one of:
    # stringValue, doubleValue, booleanValue, emptyValue, userArnValue
    raw_fields = response.get("fields", [])
    fields: Dict[str, Any] = {}
    for field in raw_fields:
        field_id = field.get("id", "")
        value_obj = field.get("value", {})
        if "stringValue" in value_obj:
            fields[field_id] = value_obj["stringValue"]
        elif "doubleValue" in value_obj:
            fields[field_id] = value_obj["doubleValue"]
        elif "booleanValue" in value_obj:
            fields[field_id] = value_obj["booleanValue"]
        elif "userArnValue" in value_obj:
            fields[field_id] = value_obj["userArnValue"]
        elif "emptyValue" in value_obj:
            fields[field_id] = None
        else:
            fields[field_id] = str(value_obj)
            logger.warning("Unknown value type for field %s: %s", field_id, value_obj)

    result = {
        "channel": "TASK",
        "contact_id": contact_id,
        "case_id": case_id,
        "task_name": task_name,
        "fields": fields,
    }

    logger.info("TASK processing complete: case_id=%s, fields_count=%d", case_id, len(fields))
    return result


def process_chat_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process a CHAT channel event: extract contact attributes.

    Returns a dict with:
      - channel: "CHAT"
      - contact_id: the ContactId from the event
      - fields: dict of contact attributes from ContactData.Attributes
    """
    contact_data = event.get("Details", {}).get("ContactData", {})
    contact_id = contact_data.get("ContactId", "")
    attributes = contact_data.get("Attributes", {})

    logger.info("Processing CHAT event: contact_id=%s, attributes_count=%d",
                contact_id, len(attributes))

    result = {
        "channel": "CHAT",
        "contact_id": contact_id,
        "fields": attributes,
    }

    logger.info("CHAT processing complete: contact_id=%s, fields_count=%d",
                contact_id, len(attributes))
    return result


def send_sns_notification(data: Dict[str, Any], sns_topic_arn: str) -> Optional[str]:
    """Publish a notification to SNS with the contact/case data as JSON.

    Args:
        data: The dict returned by process_task_event or process_chat_event.
        sns_topic_arn: ARN of the SNS topic to publish to.

    Returns:
        The SNS MessageId on success, or None on failure.
    """
    if not sns_topic_arn:
        logger.warning("SNS topic ARN not configured; skipping notification.")
        return None

    channel = data.get("channel", "UNKNOWN")
    contact_id = data.get("contact_id", "unknown")

    subject = f"Amazon Connect {channel} - Contact {contact_id}"
    # SNS email subject max is 100 characters
    if len(subject) > 100:
        subject = subject[:97] + "..."

    message_body = json.dumps(data, indent=2, default=str)

    try:
        response = _sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=message_body,
        )
        message_id = response.get("MessageId", "")
        logger.info("SNS notification sent: message_id=%s, topic=%s, channel=%s, contact=%s",
                     message_id, sns_topic_arn, channel, contact_id)
        return message_id
    except ClientError as e:
        logger.error("SNS publish failed: %s: %s", type(e).__name__, str(e))
        return None
    except Exception as e:
        logger.error("SNS publish unexpected error: %s: %s", type(e).__name__, str(e))
        return None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main entry point: route based on Channel, process, and notify via SNS.

    Invoked from an Amazon Connect contact flow. Determines the channel
    (TASK or CHAT) from the event, extracts the relevant data, publishes
    an SNS notification, and returns the extracted data to the flow.
    """
    try:
        contact_data = event.get("Details", {}).get("ContactData", {})
        channel = contact_data.get("Channel", "")
        contact_id = contact_data.get("ContactId", "unknown")

        logger.info("Lambda invoked: channel=%s, contact_id=%s", channel, contact_id)

        if channel == "TASK":
            data = process_task_event(event)
        elif channel == "CHAT":
            data = process_chat_event(event)
        else:
            logger.error("Unsupported channel: %s for contact %s", channel, contact_id)
            return {
                "status": "error",
                "error": f"Unsupported channel: {channel}",
                "contact_id": contact_id,
            }

        # Send SNS notification
        message_id = send_sns_notification(data, _SNS_TOPIC_ARN)

        # Return the data to the Connect flow as flat key-value pairs
        result: Dict[str, str] = {
            "status": "success",
            "channel": data.get("channel", ""),
            "contact_id": data.get("contact_id", ""),
            "sns_message_id": message_id or "not_sent",
        }
        if "case_id" in data:
            result["case_id"] = data["case_id"]
        if "task_name" in data:
            result["task_name"] = data["task_name"]

        # Flatten fields into the return value for Connect contact attributes.
        # Prefix with "case_" or "chat_" to avoid collisions with top-level keys
        # like "status" and "channel".
        prefix = "case_" if channel == "TASK" else "chat_"
        for key, value in data.get("fields", {}).items():
            result[f"{prefix}{key}"] = str(value) if value is not None else ""

        logger.info("Lambda complete: channel=%s, contact_id=%s, fields=%d, sns=%s",
                     channel, contact_id, len(data.get("fields", {})),
                     "sent" if message_id else "skipped")
        return result

    except ValueError as e:
        logger.error("Validation error: %s: %s", type(e).__name__, str(e))
        return {
            "status": "error",
            "error": str(e),
        }
    except Exception as e:
        logger.error("Fatal error: %s: %s", type(e).__name__, str(e))
        raise
