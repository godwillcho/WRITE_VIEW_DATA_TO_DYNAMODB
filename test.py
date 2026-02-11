"""
Lambda Function: Normalize View Result Data for Amazon Connect

Converts multi-select answers in `Details.Parameters.viewResultData` into a
single comma-separated string (e.g., {"0":"A","1":"B"} → "A, B"), while leaving
scalars unchanged. Emits structured, non-sensitive logs (info/debug/error).

Also writes the result to DynamoDB when configured (env: DDB_TABLE_NAME, DDB_PK_NAME):
- Partition key (DDB_PK_NAME) = InitialContactId from ContactData.
- Contact IDs: ContactId, PreviousContactId, RelatedContactId.
- Endpoints: CustomerEndpointAddress, SystemEndpointAddress, OutboundCallerId.
- Flow metadata: InitiationMethod, Channel, QueueName.
- Agent (via describe_contact + describe_user): AgentId, AgentName, AgentUsername.
- Timestamps (via describe_contact): InitiationTimestamp, DisconnectTimestamp,
  ConnectedToSystemTimestamp, ConnectedToAgentTimestamp, EnqueueTimestamp, etc.
- Campaign (via describe_contact + describe_campaign): CampaignId, CampaignName.
- Normalized view result data spread as top-level attributes.
- CreatedAt: human-readable timestamp in configured timezone (env: TZ_OFFSET_HOURS, TZ_LABEL).
- TTL ("ttl"): 1 year from now.
"""

import os
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

# ---------- Logging (non-sensitive) ----------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))  # DEBUG | INFO | WARNING | ERROR

# ---------- DynamoDB (config via env) ----------
_DDB_TABLE = os.getenv("DDB_TABLE_NAME")  # required to enable writes
_DDB_PK = os.getenv("DDB_PK_NAME")        # required to enable writes
_DDB_TTL_ATTR = "ttl"                     # table's TTL attribute name (must be configured on the table)
try:
    _DDB_TZ_OFFSET = int(os.getenv("TZ_OFFSET_HOURS", "-5"))
except ValueError:
    _DDB_TZ_OFFSET = -5  # fallback to EST
_DDB_TZ_LABEL = os.getenv("TZ_LABEL", "EST")              # label shown in the timestamp string
_VQ_TABLE = os.getenv("VIEW_QUESTIONS_TABLE")
_dynamodb = boto3.resource("dynamodb") if _DDB_TABLE and _DDB_PK else None
_connect_client = boto3.client("connect")
_campaigns_client = boto3.client("connectcampaigns")


def normalize_view_value(raw_value: Any) -> Any:
    """Convert multi-select values into a single comma-separated string."""
    if isinstance(raw_value, dict) and all(str(k).isdigit() for k in raw_value.keys()):
        ordered_items: List[Any] = [raw_value[k] for k in sorted(raw_value.keys(), key=lambda x: int(x))]
        return ", ".join(map(str, ordered_items))
    if isinstance(raw_value, list):
        return ", ".join(map(str, raw_value))
    return raw_value


def _get_contact_details(instance_arn: str, contact_id: str) -> Dict[str, Any]:
    """Follow RelatedContactId chain to find the original VOICE contact,
    then extract agent info, timestamps, channel, and campaign data."""
    result: Dict[str, Any] = {}
    try:
        instance_id = instance_arn.split("/")[-1]

        # Follow the RelatedContactId chain to find the original VOICE contact (max 5 hops)
        current_id = contact_id
        contact = {}
        for hop in range(5):
            contact_resp = _connect_client.describe_contact(
                InstanceId=instance_id,
                ContactId=current_id,
            )
            contact = contact_resp.get("Contact", {})
            channel = contact.get("Channel", "")
            logger.info("Chain hop %d: contact=%s channel=%s", hop, current_id, channel)

            if channel == "VOICE":
                break  # Found the original voice contact

            next_related = contact.get("RelatedContactId")
            if not next_related or next_related == current_id:
                break  # No more related contacts to follow
            current_id = next_related

        # Store which contact we extracted details from and its channel
        result["DescribedContactId"] = current_id
        channel = contact.get("Channel")
        if channel:
            result["OriginalContactChannel"] = channel

        # --- Timestamps (convert datetime objects to ISO strings) ---
        timestamp_fields = [
            "InitiationTimestamp",
            "DisconnectTimestamp",
            "ConnectedToSystemTimestamp",
            "LastUpdateTimestamp",
            "LastPausedTimestamp",
            "LastResumedTimestamp",
            "ScheduledTimestamp",
        ]
        for ts_field in timestamp_fields:
            ts_val = contact.get(ts_field)
            if ts_val:
                result[ts_field] = ts_val.isoformat() if hasattr(ts_val, "isoformat") else str(ts_val)

        # Agent-level timestamps
        agent_info = contact.get("AgentInfo", {}) or {}
        agent_ts_fields = [
            "ConnectedToAgentTimestamp",
            "AcceptedByAgentTimestamp",
            "AfterContactWorkStartTimestamp",
            "AfterContactWorkEndTimestamp",
        ]
        for ts_field in agent_ts_fields:
            ts_val = agent_info.get(ts_field)
            if ts_val:
                result[ts_field] = ts_val.isoformat() if hasattr(ts_val, "isoformat") else str(ts_val)

        # Queue enqueue timestamp
        queue_info = contact.get("QueueInfo", {}) or {}
        enqueue_ts = queue_info.get("EnqueueTimestamp")
        if enqueue_ts:
            result["EnqueueTimestamp"] = enqueue_ts.isoformat() if hasattr(enqueue_ts, "isoformat") else str(enqueue_ts)

        # --- Agent identity ---
        agent_id = agent_info.get("Id")
        if agent_id:
            result["AgentId"] = agent_id
            try:
                user_resp = _connect_client.describe_user(
                    InstanceId=instance_id,
                    UserId=agent_id,
                )
                identity = user_resp.get("User", {}).get("IdentityInfo", {})
                first_name = identity.get("FirstName", "")
                last_name = identity.get("LastName", "")
                if first_name or last_name:
                    result["AgentName"] = f"{first_name} {last_name}".strip()
                username = user_resp.get("User", {}).get("Username")
                if username:
                    result["AgentUsername"] = username
            except Exception as e:
                logger.error("describe_user failed: %s: %s", type(e).__name__, str(e))
        else:
            logger.info("No agent assigned to contact %s.", current_id)

        # --- Campaign info ---
        campaign = contact.get("Campaign", {}) or {}
        campaign_id = campaign.get("CampaignId")
        if campaign_id:
            result["CampaignId"] = campaign_id
            try:
                camp_resp = _campaigns_client.describe_campaign(id=campaign_id)
                camp_name = camp_resp.get("campaign", {}).get("name")
                if camp_name:
                    result["CampaignName"] = camp_name
            except Exception as e:
                logger.error("describe_campaign failed: %s: %s", type(e).__name__, str(e))

        logger.info("describe_contact success: described=%s channel=%s fields=%d",
                     current_id, contact.get("Channel"), len(result))

    except ClientError as e:
        logger.error("describe_contact ClientError: %s: %s", type(e).__name__, str(e))
    except Exception as e:
        logger.error("describe_contact failed: %s: %s", type(e).__name__, str(e))

    return result


def _write_to_dynamodb(item: Dict[str, Any]) -> None:
    """Write item to DynamoDB with PK = InitialContactId plus contact attributes and 1-year TTL."""
    if not _dynamodb:
        logger.info("DynamoDB not configured: skipping write (missing DDB_TABLE_NAME or DDB_PK_NAME).")
        return

    try:
        table = _dynamodb.Table(_DDB_TABLE)
        table.put_item(Item=item)
        logger.info("DynamoDB write success: table=%s pk=%s ttl_days=365",
                    _DDB_TABLE, item.get(_DDB_PK, "unknown"))
    except ClientError as e:
        logger.error("DynamoDB ClientError: %s: %s", type(e).__name__, str(e))
    except Exception as e:
        logger.error("DynamoDB write failed: %s: %s", type(e).__name__, str(e))


def lambda_handler(event, context):
    """Normalize multi-select answers in viewResultData for use in Amazon Connect flows."""
    try:
        event_details: Dict[str, Any] = event.get("Details", {}) or {}
        parameters: Dict[str, Any] = event_details.get("Parameters", {}) or {}

        view_result_data: Dict[str, Any] = parameters.get("viewResultData", {}) or {}
        view_action: str = parameters.get("viewAction", "") or ""

        key_count = len(view_result_data)
        logger.info("Normalization start: keys=%d, action_present=%s", key_count, bool(view_action))

        normalized_attributes: Dict[str, Any] = {}
        per_key_errors = 0

        for answer_key, raw_answer_value in view_result_data.items():
            try:
                is_numeric_dict = isinstance(raw_answer_value, dict) and all(str(k).isdigit() for k in raw_answer_value.keys())
                is_list = isinstance(raw_answer_value, list)
                classification = "numeric_dict" if is_numeric_dict else ("list" if is_list else "scalar")
                logger.debug("Normalizing key='%s' type=%s", answer_key, classification)

                normalized_attributes[answer_key] = normalize_view_value(raw_answer_value)

            except Exception as key_error:
                per_key_errors += 1
                # Do not log raw values; include only key name and error type/message.
                logger.error("Normalization failed for key='%s': %s: %s",
                             answer_key, type(key_error).__name__, str(key_error))
                # Best-effort: keep original value to avoid data loss
                normalized_attributes[answer_key] = raw_answer_value

        if view_action:
            normalized_attributes["viewAction"] = view_action

        logger.info("Normalization complete: normalized_keys=%d, key_errors=%d",
                    len(normalized_attributes), per_key_errors)

        # --- Write to DynamoDB: PK = InitialContactId; store contact IDs, customer address, and normalized data ---
        contact_data: Dict[str, Any] = event_details.get("ContactData", {}) or {}
        initial_contact_id = contact_data.get("InitialContactId")

        if initial_contact_id:
            ttl_value = int(time.time()) + 365 * 24 * 3600  # 1 year from now

            item: Dict[str, Any] = {
                _DDB_PK: initial_contact_id,
                _DDB_TTL_ATTR: ttl_value,
            }

            # Spread normalized attributes as top-level DynamoDB attributes
            item.update(normalized_attributes)

            # Add optional contact attributes (only written if present and non-None)
            optional_fields = {
                "ContactId":              contact_data.get("ContactId"),
                "PreviousContactId":      contact_data.get("PreviousContactId"),
                "RelatedContactId":       contact_data.get("RelatedContactId"),
                "CustomerEndpointAddress": contact_data.get("CustomerEndpoint", {}).get("Address"),
                "InitiationMethod":       contact_data.get("InitiationMethod"),
                "Channel":                contact_data.get("Channel"),
                "QueueName":              contact_data.get("Queue", {}).get("Name") if contact_data.get("Queue") else None,
                "SystemEndpointAddress":  contact_data.get("SystemEndpoint", {}).get("Address") if contact_data.get("SystemEndpoint") else None,
                "OutboundCallerId":       contact_data.get("Queue", {}).get("OutboundCallerId") if contact_data.get("Queue") else None,
            }
            for attr_name, attr_value in optional_fields.items():
                if attr_value:
                    item[attr_name] = attr_value

            # Human-readable timestamp in configured timezone for when this record was created
            tz = timezone(timedelta(hours=_DDB_TZ_OFFSET))
            item["CreatedAt"] = datetime.now(tz).strftime(f"%B %d, %Y %I:%M:%S %p {_DDB_TZ_LABEL}")

            # Lookup agent info, timestamps, campaign, and channel from the related contact (original voice call)
            instance_arn = contact_data.get("InstanceARN")
            related_contact_id = contact_data.get("RelatedContactId")
            if instance_arn and related_contact_id:
                contact_details = _get_contact_details(instance_arn, related_contact_id)
                item.update(contact_details)

            # Lookup question text for each normalized key from ViewQuestions table
            if _VQ_TABLE and _dynamodb:
                vq_table = _dynamodb.Table(_VQ_TABLE)
                for attr_key in list(normalized_attributes.keys()):
                    if attr_key == "viewAction":
                        continue
                    try:
                        resp = vq_table.query(
                            KeyConditionExpression="#n = :name",
                            ExpressionAttributeNames={"#n": "Name"},
                            ExpressionAttributeValues={":name": attr_key},
                            Limit=1,
                        )
                        vq_items = resp.get("Items", [])
                        if vq_items:
                            question_label = vq_items[0].get("Label", "")
                            if question_label:
                                item[f"{attr_key}_Question"] = question_label
                    except Exception as e:
                        logger.error("ViewQuestions query failed for key=%s: %s: %s",
                                     attr_key, type(e).__name__, str(e))

            _write_to_dynamodb(item)
        else:
            logger.warning("InitialContactId not found in event; skipping DynamoDB write.")

        return normalized_attributes

    except Exception as fatal_error:
        # Fatal handler errors: log without dumping the event payload.
        logger.error("Fatal error during normalization: %s: %s",
                     type(fatal_error).__name__, str(fatal_error))
        # Re-raise so the flow can branch on Lambda error if desired.
        raise