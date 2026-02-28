"""
Script: Enable Amazon Connect Case Event Streams for Contact Lens Rules

Follows the AWS documentation to allow Cases to send updates to Contact Lens rules:
  Step 1: Enable case event streams (put-case-event-configuration)
  Step 2: Create an event integration (create-event-integration)
  Step 3: Associate the integration with your Connect instance (create-integration-association)

Ref: https://docs.aws.amazon.com/connect/latest/adminguide/cases-rules-integration-onboarding.html

Usage:
  # Full setup (enable streams + Contact Lens rules integration)
  python enable_case_event_streams.py \
    --instance-id YOUR_CONNECT_INSTANCE_ID \
    --domain-id YOUR_CASES_DOMAIN_ID \
    --region us-west-2

  # Include custom case fields in event payloads
  python enable_case_event_streams.py \
    --instance-id YOUR_CONNECT_INSTANCE_ID \
    --domain-id YOUR_CASES_DOMAIN_ID \
    --region us-west-2 \
    --extra-fields my_custom_field_1 my_custom_field_2

  # List all available case fields for your domain
  python enable_case_event_streams.py \
    --instance-id YOUR_CONNECT_INSTANCE_ID \
    --domain-id YOUR_CASES_DOMAIN_ID \
    --region us-west-2 \
    --list-fields

  # Optional: also route case events to an SQS queue
  python enable_case_event_streams.py \
    --instance-id YOUR_CONNECT_INSTANCE_ID \
    --domain-id YOUR_CASES_DOMAIN_ID \
    --region us-west-2 \
    --create-sqs-route
"""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- Default case fields included in event payloads ----------
# These match the AWS documentation for Contact Lens rules integration.
DEFAULT_CASE_FIELDS = [
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


def enable_case_event_streams(
    cases_client: Any,
    domain_id: str,
    extra_fields: Optional[List[str]] = None,
) -> None:
    """Step 1: Enable case event streams via PutCaseEventConfiguration.

    Configures Cases to publish CASE.CREATED, CASE.UPDATED, and
    RELATED_ITEM.CREATED events to EventBridge.

    relatedItemData.includeContent is set to True as required for
    Cases SLA Breach rules to work properly.
    """
    fields = [{"id": f} for f in DEFAULT_CASE_FIELDS]
    if extra_fields:
        for f in extra_fields:
            if f not in DEFAULT_CASE_FIELDS:
                fields.append({"id": f})

    logger.info("Step 1: Enabling case event streams for domain %s with %d fields.", domain_id, len(fields))

    cases_client.put_case_event_configuration(
        domainId=domain_id,
        eventBridge={
            "enabled": True,
            "includedData": {
                "caseData": {
                    "fields": fields,
                },
                "relatedItemData": {
                    "includeContent": True,
                },
            },
        },
    )
    logger.info("Step 1 complete: case event streams enabled.")

    # Verify
    config = cases_client.get_case_event_configuration(domainId=domain_id)
    enabled = config.get("eventBridge", {}).get("enabled", False)
    logger.info("Verified: eventBridge.enabled=%s", enabled)


def create_event_integration(
    appintegrations_client: Any,
    integration_name: str = "amazon-connect-cases",
) -> str:
    """Step 2: Create an event integration for Cases -> Contact Lens rules.

    Uses the AppIntegrations service to create an integration that listens
    for aws.cases events on the default EventBridge bus.
    """
    # Check if it already exists
    try:
        resp = appintegrations_client.get_event_integration(Name=integration_name)
        arn = resp["EventIntegrationArn"]
        logger.info("Step 2: Event integration already exists: %s", arn)
        return arn
    except appintegrations_client.exceptions.ResourceNotFoundException:
        pass
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info("Step 2: Creating event integration: %s", integration_name)
    resp = appintegrations_client.create_event_integration(
        Name=integration_name,
        Description=integration_name,
        EventFilter={"Source": "aws.cases"},
        EventBridgeBus="default",
    )
    arn = resp["EventIntegrationArn"]
    logger.info("Step 2 complete: event integration created: %s", arn)
    return arn


def associate_integration_with_instance(
    connect_client: Any,
    instance_id: str,
    event_integration_arn: str,
) -> str:
    """Step 3: Associate the event integration with the Connect instance.

    This is what enables Contact Lens rules to use OnCaseCreate,
    OnCaseUpdate, and OnSlaBreach as trigger event sources.
    """
    # Check if already associated
    try:
        paginator = connect_client.get_paginator("list_integration_associations")
        for page in paginator.paginate(InstanceId=instance_id, IntegrationType="EVENT"):
            for assoc in page.get("IntegrationAssociationSummaryList", []):
                if assoc.get("IntegrationArn") == event_integration_arn:
                    assoc_id = assoc["IntegrationAssociationId"]
                    logger.info("Step 3: Integration already associated: %s", assoc_id)
                    return assoc_id
    except ClientError:
        pass

    logger.info("Step 3: Creating integration association for instance %s.", instance_id)
    resp = connect_client.create_integration_association(
        InstanceId=instance_id,
        IntegrationType="EVENT",
        IntegrationArn=event_integration_arn,
        SourceType="CASES",
    )
    assoc_id = resp["IntegrationAssociationId"]
    logger.info("Step 3 complete: integration association created: %s", assoc_id)
    return assoc_id


def create_sqs_route(
    events_client: Any,
    sqs_client: Any,
    sts_client: Any,
    region: str,
    rule_name: str = "case-events-to-sqs",
    queue_name: str = "case-events-queue",
) -> Dict[str, str]:
    """Optional: Create EventBridge rule -> SQS queue for case events.

    Not required for Contact Lens rules. Use this only if you also want
    to consume case events in a downstream system (e.g. for logging or analytics).
    """
    account_id = sts_client.get_caller_identity()["Account"]
    queue_arn = f"arn:aws:sqs:{region}:{account_id}:{queue_name}"

    sqs_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowEventBridge",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "SQS:SendMessage",
            "Resource": queue_arn,
        }],
    })

    logger.info("Creating SQS queue: %s", queue_name)
    try:
        queue_resp = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={"Policy": sqs_policy},
        )
        queue_url = queue_resp["QueueUrl"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "QueueAlreadyExists":
            queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
            logger.info("SQS queue already exists: %s", queue_url)
        else:
            raise

    attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = attrs["Attributes"]["QueueArn"]

    logger.info("Creating EventBridge rule: %s", rule_name)
    events_client.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({
            "source": ["aws.cases"],
            "detail-type": ["Amazon Connect Cases Change"],
        }),
        State="ENABLED",
        Description="Route Amazon Connect case events to SQS",
    )

    events_client.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
    )
    logger.info("EventBridge rule -> SQS target configured.")

    return {"rule_name": rule_name, "queue_url": queue_url, "queue_arn": queue_arn}


def list_case_fields(cases_client: Any, domain_id: str) -> None:
    """List all available case fields for a domain.

    Use this to find custom field IDs to pass with --extra-fields.
    """
    logger.info("Listing case fields for domain %s:", domain_id)
    paginator = cases_client.get_paginator("list_fields")
    for page in paginator.paginate(domainId=domain_id):
        for field in page.get("fields", []):
            field_id = field.get("fieldId", "")
            name = field.get("name", "")
            field_type = field.get("type", "")
            logger.info("  %-40s %-30s %s", field_id, name, field_type)


def main():
    parser = argparse.ArgumentParser(
        description="Enable Amazon Connect Case Event Streams for Contact Lens Rules.",
    )
    parser.add_argument("--instance-id", required=True, help="Amazon Connect instance ID")
    parser.add_argument("--domain-id", required=True, help="Amazon Connect Cases domain ID")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument("--extra-fields", nargs="*", default=[], help="Additional case field IDs to include in events")
    parser.add_argument("--list-fields", action="store_true", help="List available case fields and exit")
    parser.add_argument("--create-sqs-route", action="store_true",
                        help="Optional: also create EventBridge -> SQS route (not needed for Contact Lens rules)")
    parser.add_argument(
        "--integration-name",
        default="amazon-connect-cases",
        help="Name for the event integration (default: amazon-connect-cases)",
    )

    args = parser.parse_args()

    # Initialize clients
    session = boto3.Session(region_name=args.region)
    cases_client = session.client("connectcases")
    connect_client = session.client("connect")
    appintegrations_client = session.client("appintegrations")

    # List fields and exit
    if args.list_fields:
        list_case_fields(cases_client, args.domain_id)
        return

    logger.info("Region:      %s", args.region)
    logger.info("Instance ID: %s", args.instance_id)
    logger.info("Domain ID:   %s", args.domain_id)

    # Step 1: Enable case event streams
    try:
        enable_case_event_streams(cases_client, args.domain_id, args.extra_fields)
    except ClientError as e:
        logger.error("Failed at Step 1 (enable case event streams): %s", e)
        sys.exit(1)

    # Step 2: Create event integration
    try:
        event_integration_arn = create_event_integration(
            appintegrations_client, args.integration_name
        )
    except ClientError as e:
        logger.error("Failed at Step 2 (create event integration): %s", e)
        sys.exit(1)

    # Step 3: Associate integration with Connect instance
    try:
        associate_integration_with_instance(
            connect_client, args.instance_id, event_integration_arn
        )
    except ClientError as e:
        logger.error("Failed at Step 3 (associate integration): %s", e)
        sys.exit(1)

    logger.info("All 3 steps complete. Contact Lens rules can now use OnCaseCreate, OnCaseUpdate, and OnSlaBreach triggers.")

    # Optional: EventBridge -> SQS route
    if args.create_sqs_route:
        try:
            events_client = session.client("events")
            sqs_client = session.client("sqs")
            sts_client = session.client("sts")
            sqs_result = create_sqs_route(events_client, sqs_client, sts_client, args.region)
            logger.info("SQS queue URL: %s", sqs_result["queue_url"])
        except ClientError as e:
            logger.error("Failed to create SQS route: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    main()
