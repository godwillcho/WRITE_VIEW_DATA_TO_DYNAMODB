"""Tests for enable_case_event_streams.py using mocks (no AWS credentials needed)."""

import sys
import unittest
from unittest.mock import MagicMock, patch, call

from enable_case_event_streams import (
    enable_case_event_streams,
    create_event_integration,
    associate_integration_with_instance,
    create_sqs_route,
    list_case_fields,
    DEFAULT_CASE_FIELDS,
)


class TestEnableCaseEventStreams(unittest.TestCase):
    """Step 1: PutCaseEventConfiguration."""

    def test_enables_streams_with_default_fields(self):
        client = MagicMock()
        client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }

        enable_case_event_streams(client, "domain-123")

        client.put_case_event_configuration.assert_called_once()
        call_kwargs = client.put_case_event_configuration.call_args[1]
        assert call_kwargs["domainId"] == "domain-123"
        assert call_kwargs["eventBridge"]["enabled"] is True
        fields = call_kwargs["eventBridge"]["includedData"]["caseData"]["fields"]
        assert len(fields) == len(DEFAULT_CASE_FIELDS)
        assert call_kwargs["eventBridge"]["includedData"]["relatedItemData"]["includeContent"] is True
        print("PASS: enables streams with default fields")

    def test_includes_extra_fields(self):
        client = MagicMock()
        client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }

        enable_case_event_streams(client, "domain-123", extra_fields=["custom_field_1", "custom_field_2"])

        call_kwargs = client.put_case_event_configuration.call_args[1]
        fields = call_kwargs["eventBridge"]["includedData"]["caseData"]["fields"]
        assert len(fields) == len(DEFAULT_CASE_FIELDS) + 2
        field_ids = [f["id"] for f in fields]
        assert "custom_field_1" in field_ids
        assert "custom_field_2" in field_ids
        print("PASS: includes extra fields")

    def test_deduplicates_extra_fields(self):
        client = MagicMock()
        client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }

        # "status" is already in DEFAULT_CASE_FIELDS
        enable_case_event_streams(client, "domain-123", extra_fields=["status", "new_field"])

        call_kwargs = client.put_case_event_configuration.call_args[1]
        fields = call_kwargs["eventBridge"]["includedData"]["caseData"]["fields"]
        assert len(fields) == len(DEFAULT_CASE_FIELDS) + 1  # only new_field added
        print("PASS: deduplicates extra fields already in defaults")

    def test_verifies_configuration_after_enabling(self):
        client = MagicMock()
        client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }

        enable_case_event_streams(client, "domain-123")

        client.get_case_event_configuration.assert_called_once_with(domainId="domain-123")
        print("PASS: verifies configuration after enabling")


class TestCreateEventIntegration(unittest.TestCase):
    """Step 2: Create event integration."""

    def test_creates_new_integration(self):
        client = MagicMock()
        client.exceptions.ResourceNotFoundException = type("ResourceNotFoundException", (Exception,), {})
        client.get_event_integration.side_effect = client.exceptions.ResourceNotFoundException()
        client.create_event_integration.return_value = {
            "EventIntegrationArn": "arn:aws:app-integrations:us-west-2:123456789012:event-integration/amazon-connect-cases"
        }

        arn = create_event_integration(client)

        client.create_event_integration.assert_called_once()
        call_kwargs = client.create_event_integration.call_args[1]
        assert call_kwargs["Name"] == "amazon-connect-cases"
        assert call_kwargs["EventFilter"] == {"Source": "aws.cases"}
        assert call_kwargs["EventBridgeBus"] == "default"
        assert "event-integration" in arn
        print("PASS: creates new event integration")

    def test_returns_existing_integration(self):
        client = MagicMock()
        existing_arn = "arn:aws:app-integrations:us-west-2:123456789012:event-integration/amazon-connect-cases"
        client.get_event_integration.return_value = {
            "EventIntegrationArn": existing_arn
        }

        arn = create_event_integration(client)

        assert arn == existing_arn
        client.create_event_integration.assert_not_called()
        print("PASS: returns existing integration without creating")

    def test_custom_integration_name(self):
        client = MagicMock()
        client.exceptions.ResourceNotFoundException = type("ResourceNotFoundException", (Exception,), {})
        client.get_event_integration.side_effect = client.exceptions.ResourceNotFoundException()
        client.create_event_integration.return_value = {
            "EventIntegrationArn": "arn:aws:app-integrations:us-west-2:123456789012:event-integration/my-custom-name"
        }

        arn = create_event_integration(client, integration_name="my-custom-name")

        call_kwargs = client.create_event_integration.call_args[1]
        assert call_kwargs["Name"] == "my-custom-name"
        print("PASS: uses custom integration name")


class TestAssociateIntegration(unittest.TestCase):
    """Step 3: Associate integration with Connect instance."""

    def test_creates_new_association(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"IntegrationAssociationSummaryList": []}
        ]
        client.get_paginator.return_value = paginator
        client.create_integration_association.return_value = {
            "IntegrationAssociationId": "assoc-12345"
        }

        assoc_id = associate_integration_with_instance(
            client, "instance-id", "arn:aws:event-integration/test"
        )

        assert assoc_id == "assoc-12345"
        call_kwargs = client.create_integration_association.call_args[1]
        assert call_kwargs["InstanceId"] == "instance-id"
        assert call_kwargs["IntegrationType"] == "EVENT"
        assert call_kwargs["SourceType"] == "CASES"
        print("PASS: creates new integration association")

    def test_returns_existing_association(self):
        client = MagicMock()
        target_arn = "arn:aws:event-integration/test"
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"IntegrationAssociationSummaryList": [
                {
                    "IntegrationArn": target_arn,
                    "IntegrationAssociationId": "existing-assoc-id"
                }
            ]}
        ]
        client.get_paginator.return_value = paginator

        assoc_id = associate_integration_with_instance(
            client, "instance-id", target_arn
        )

        assert assoc_id == "existing-assoc-id"
        client.create_integration_association.assert_not_called()
        print("PASS: returns existing association without creating")


class TestCreateSqsRoute(unittest.TestCase):
    """Optional: EventBridge -> SQS route."""

    def test_creates_sqs_route(self):
        events_client = MagicMock()
        sqs_client = MagicMock()
        sts_client = MagicMock()

        sts_client.get_caller_identity.return_value = {"Account": "123456789012"}
        sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/case-events-queue"
        }
        sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-west-2:123456789012:case-events-queue"}
        }

        result = create_sqs_route(events_client, sqs_client, sts_client, "us-west-2")

        assert result["queue_url"] == "https://sqs.us-west-2.amazonaws.com/123456789012/case-events-queue"
        assert result["rule_name"] == "case-events-to-sqs"
        events_client.put_rule.assert_called_once()
        events_client.put_targets.assert_called_once()
        print("PASS: creates SQS route with EventBridge rule and target")

    def test_handles_existing_queue(self):
        from botocore.exceptions import ClientError

        events_client = MagicMock()
        sqs_client = MagicMock()
        sts_client = MagicMock()

        sts_client.get_caller_identity.return_value = {"Account": "123456789012"}
        sqs_client.create_queue.side_effect = ClientError(
            {"Error": {"Code": "QueueAlreadyExists", "Message": "Queue already exists"}},
            "CreateQueue"
        )
        sqs_client.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/case-events-queue"
        }
        sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-west-2:123456789012:case-events-queue"}
        }

        result = create_sqs_route(events_client, sqs_client, sts_client, "us-west-2")

        assert "queue_url" in result
        sqs_client.get_queue_url.assert_called_once()
        print("PASS: handles existing SQS queue gracefully")


class TestListCaseFields(unittest.TestCase):
    """Helper: list case fields."""

    def test_lists_fields(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"fields": [
                {"fieldId": "status", "name": "Status", "type": "SingleSelect"},
                {"fieldId": "title", "name": "Title", "type": "Text"},
                {"fieldId": "custom_1", "name": "My Custom Field", "type": "Text"},
            ]}
        ]
        client.get_paginator.return_value = paginator

        # Should not raise
        list_case_fields(client, "domain-123")

        client.get_paginator.assert_called_once_with("list_fields")
        print("PASS: lists case fields without error")


class TestMainCLI(unittest.TestCase):
    """End-to-end CLI flow."""

    @patch("enable_case_event_streams.boto3.Session")
    def test_default_runs_all_three_steps(self, mock_session_cls):
        """Default invocation runs Steps 1-3 (no SQS)."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        cases_client = MagicMock()
        connect_client = MagicMock()
        appintegrations_client = MagicMock()

        def client_factory(service_name):
            return {
                "connectcases": cases_client,
                "connect": connect_client,
                "appintegrations": appintegrations_client,
            }[service_name]

        mock_session.client.side_effect = client_factory

        # Step 1 mock
        cases_client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }

        # Step 2 mock
        appintegrations_client.exceptions.ResourceNotFoundException = type("ResourceNotFoundException", (Exception,), {})
        appintegrations_client.get_event_integration.side_effect = (
            appintegrations_client.exceptions.ResourceNotFoundException()
        )
        appintegrations_client.create_event_integration.return_value = {
            "EventIntegrationArn": "arn:aws:app-integrations:us-west-2:123:event-integration/amazon-connect-cases"
        }

        # Step 3 mock
        paginator = MagicMock()
        paginator.paginate.return_value = [{"IntegrationAssociationSummaryList": []}]
        connect_client.get_paginator.return_value = paginator
        connect_client.create_integration_association.return_value = {
            "IntegrationAssociationId": "assoc-abc"
        }

        sys.argv = [
            "enable_case_event_streams.py",
            "--instance-id", "inst-123",
            "--domain-id", "dom-456",
            "--region", "us-west-2",
        ]

        from enable_case_event_streams import main
        main()

        # Verify all 3 steps were called
        cases_client.put_case_event_configuration.assert_called_once()
        appintegrations_client.create_event_integration.assert_called_once()
        connect_client.create_integration_association.assert_called_once()
        print("PASS: default CLI runs all 3 steps")

    @patch("enable_case_event_streams.boto3.Session")
    def test_sqs_route_only_when_flagged(self, mock_session_cls):
        """SQS route is NOT created unless --create-sqs-route is passed."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        cases_client = MagicMock()
        connect_client = MagicMock()
        appintegrations_client = MagicMock()

        def client_factory(service_name):
            return {
                "connectcases": cases_client,
                "connect": connect_client,
                "appintegrations": appintegrations_client,
            }[service_name]

        mock_session.client.side_effect = client_factory

        cases_client.get_case_event_configuration.return_value = {
            "eventBridge": {"enabled": True}
        }
        appintegrations_client.exceptions.ResourceNotFoundException = type("ResourceNotFoundException", (Exception,), {})
        appintegrations_client.get_event_integration.side_effect = (
            appintegrations_client.exceptions.ResourceNotFoundException()
        )
        appintegrations_client.create_event_integration.return_value = {
            "EventIntegrationArn": "arn:aws:app-integrations:us-west-2:123:event-integration/test"
        }
        paginator = MagicMock()
        paginator.paginate.return_value = [{"IntegrationAssociationSummaryList": []}]
        connect_client.get_paginator.return_value = paginator
        connect_client.create_integration_association.return_value = {
            "IntegrationAssociationId": "assoc-abc"
        }

        sys.argv = [
            "enable_case_event_streams.py",
            "--instance-id", "inst-123",
            "--domain-id", "dom-456",
        ]

        from enable_case_event_streams import main
        main()

        # SQS clients should never be created
        services_requested = [c[0][0] for c in mock_session.client.call_args_list]
        assert "events" not in services_requested
        assert "sqs" not in services_requested
        assert "sts" not in services_requested
        print("PASS: SQS route not created without --create-sqs-route flag")

    @patch("enable_case_event_streams.boto3.Session")
    def test_list_fields_exits_early(self, mock_session_cls):
        """--list-fields should list fields and NOT run the 3 steps."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        cases_client = MagicMock()
        connect_client = MagicMock()
        appintegrations_client = MagicMock()

        def client_factory(service_name):
            return {
                "connectcases": cases_client,
                "connect": connect_client,
                "appintegrations": appintegrations_client,
            }[service_name]

        mock_session.client.side_effect = client_factory

        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"fields": [{"fieldId": "status", "name": "Status", "type": "SingleSelect"}]}
        ]
        cases_client.get_paginator.return_value = paginator

        sys.argv = [
            "enable_case_event_streams.py",
            "--instance-id", "inst-123",
            "--domain-id", "dom-456",
            "--list-fields",
        ]

        from enable_case_event_streams import main
        main()

        # Steps 1-3 should NOT be called
        cases_client.put_case_event_configuration.assert_not_called()
        appintegrations_client.create_event_integration.assert_not_called()
        connect_client.create_integration_association.assert_not_called()
        print("PASS: --list-fields exits early without running steps")


if __name__ == "__main__":
    unittest.main(verbosity=2)
