"""Tests for process_contact_flow_event.py using mocks (no AWS credentials needed)."""

import json
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

# ---------- Test event fixtures ----------

TASK_EVENT = {
    "Details": {
        "ContactData": {
            "Attributes": {},
            "AwsRegion": "us-east-1",
            "Channel": "TASK",
            "ContactId": "54af2d58-1c85-4586-8c16-1457620e9920",
            "CustomerEndpoint": None,
            "Description": "",
            "InitialContactId": "54af2d58-1c85-4586-8c16-1457620e9920",
            "InitiationMethod": "API",
            "InstanceARN": "arn:aws:connect:us-east-1:125677696735:instance/110e2f11-ec6e-44b9-83cb-32dac6722467",
            "Name": "Action-Required - Cases de4d7f2d-592d-3b68-908d-d09e5dc93471",
            "PreviousContactId": "54af2d58-1c85-4586-8c16-1457620e9920",
            "Queue": None,
            "References": {
                "taskRef": {
                    "Type": "URL",
                    "Value": "https://trident-united-way-dev.my.connect.aws/cases/agent-app/case-detail/de4d7f2d-592d-3b68-908d-d09e5dc93471",
                }
            },
            "RelatedContactId": None,
        },
        "Parameters": {},
    },
    "Name": "ContactFlowEvent",
}

LIST_FIELDS_RESPONSE = {
    "fields": [
        {"fieldId": "title", "name": "Title", "type": "Text", "namespace": "System"},
        {"fieldId": "status", "name": "Status", "type": "SingleSelect", "namespace": "System"},
        {"fieldId": "assigned_user", "name": "Assigned User", "type": "Text", "namespace": "System"},
        {"fieldId": "reference_number", "name": "Reference Number", "type": "Number", "namespace": "System"},
        {"fieldId": "summary", "name": "Summary", "type": "Text", "namespace": "System"},
        {"fieldId": "custom_priority", "name": "Priority Level", "type": "SingleSelect", "namespace": "Custom"},
    ],
    "nextToken": None,
}

GET_CASE_RESPONSE = {
    "fields": [
        {"id": "title", "value": {"stringValue": "Incorrect Shipping Address"}},
        {"id": "status", "value": {"stringValue": "Open"}},
        {"id": "assigned_user", "value": {"userArnValue": "arn:aws:connect:us-east-1:123:instance/i/agent/a"}},
        {"id": "reference_number", "value": {"doubleValue": 100123.0}},
        {"id": "summary", "value": {"emptyValue": {}}},
        {"id": "custom_priority", "value": {"stringValue": "High"}},
    ],
    "templateId": "template-123",
    "tags": {},
}


class TestExtractCaseId(unittest.TestCase):
    """Test _extract_case_id_from_task_ref helper."""

    def test_extracts_case_id_from_valid_url(self):
        from process_contact_flow_event import _extract_case_id_from_task_ref

        case_id = _extract_case_id_from_task_ref(TASK_EVENT)
        assert case_id == "de4d7f2d-592d-3b68-908d-d09e5dc93471"
        print("PASS: extracts case ID from valid taskRef URL")

    def test_returns_none_for_missing_references(self):
        from process_contact_flow_event import _extract_case_id_from_task_ref

        event = {"Details": {"ContactData": {"References": {}}}}
        case_id = _extract_case_id_from_task_ref(event)
        assert case_id is None
        print("PASS: returns None for missing taskRef")

    def test_returns_none_for_empty_url(self):
        from process_contact_flow_event import _extract_case_id_from_task_ref

        event = {"Details": {"ContactData": {"References": {"taskRef": {"Value": ""}}}}}
        case_id = _extract_case_id_from_task_ref(event)
        assert case_id is None
        print("PASS: returns None for empty taskRef URL")

    def test_returns_none_for_empty_event(self):
        from process_contact_flow_event import _extract_case_id_from_task_ref

        case_id = _extract_case_id_from_task_ref({})
        assert case_id is None
        print("PASS: returns None for empty event")

    def test_handles_url_with_trailing_slash(self):
        from process_contact_flow_event import _extract_case_id_from_task_ref

        event = {
            "Details": {
                "ContactData": {
                    "References": {
                        "taskRef": {
                            "Value": "https://example.com/cases/case-detail/case-id-123/"
                        }
                    }
                }
            }
        }
        case_id = _extract_case_id_from_task_ref(event)
        assert case_id == "case-id-123"
        print("PASS: handles URL with trailing slash")


class TestProcessTaskEvent(unittest.TestCase):
    """Test process_task_event function."""

    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    def test_discovers_all_fields_and_returns_values(self, mock_cases):
        from process_contact_flow_event import process_task_event, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.return_value = LIST_FIELDS_RESPONSE
        mock_cases.get_case.return_value = GET_CASE_RESPONSE

        result = process_task_event(TASK_EVENT)

        assert result["channel"] == "TASK"
        assert result["case_id"] == "de4d7f2d-592d-3b68-908d-d09e5dc93471"
        assert result["contact_id"] == "54af2d58-1c85-4586-8c16-1457620e9920"
        assert result["task_name"] == "Action-Required - Cases de4d7f2d-592d-3b68-908d-d09e5dc93471"
        assert result["template_id"] == "template-123"
        # Fields keyed by human-readable name (not field ID)
        assert result["fields"]["Title"] == "Incorrect Shipping Address"
        assert result["fields"]["Status"] == "Open"
        assert result["fields"]["Reference Number"] == 100123.0
        assert result["fields"]["Summary"] is None  # emptyValue
        assert "arn:aws:connect" in result["fields"]["Assigned User"]  # userArnValue
        # Custom field uses its name, not UUID
        assert result["fields"]["Priority Level"] == "High"
        mock_cases.list_fields.assert_called_once()
        mock_cases.get_case.assert_called_once()
        print("PASS: discovers all fields and returns human-readable names as keys")

    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    def test_passes_all_discovered_field_ids_to_get_case(self, mock_cases):
        from process_contact_flow_event import process_task_event, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.return_value = LIST_FIELDS_RESPONSE
        mock_cases.get_case.return_value = GET_CASE_RESPONSE

        process_task_event(TASK_EVENT)

        # Verify get_case received all 6 field IDs from list_fields
        call_kwargs = mock_cases.get_case.call_args[1]
        field_ids = [f["id"] for f in call_kwargs["fields"]]
        assert "title" in field_ids
        assert "status" in field_ids
        assert "custom_priority" in field_ids
        assert len(field_ids) == 6
        print("PASS: passes all discovered field IDs to get_case")

    @patch("process_contact_flow_event._CASES_DOMAIN_ID", None)
    def test_raises_when_domain_id_missing(self):
        from process_contact_flow_event import process_task_event

        with self.assertRaises(ValueError) as ctx:
            process_task_event(TASK_EVENT)
        assert "CASES_DOMAIN_ID" in str(ctx.exception)
        print("PASS: raises ValueError when CASES_DOMAIN_ID not set")

    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    def test_raises_when_case_id_not_found(self):
        from process_contact_flow_event import process_task_event

        event = {
            "Details": {
                "ContactData": {
                    "Channel": "TASK",
                    "ContactId": "contact-123",
                    "Name": "Task without case ref",
                    "References": {},
                }
            }
        }
        with self.assertRaises(ValueError) as ctx:
            process_task_event(event)
        assert "Could not extract Case ID" in str(ctx.exception)
        print("PASS: raises ValueError when Case ID cannot be extracted")

    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    def test_raises_on_get_case_error(self, mock_cases):
        from process_contact_flow_event import process_task_event, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.return_value = LIST_FIELDS_RESPONSE
        mock_cases.get_case.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Case not found"}},
            "GetCase",
        )

        with self.assertRaises(ClientError):
            process_task_event(TASK_EVENT)
        print("PASS: re-raises ClientError from get_case")

    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    def test_raises_on_list_fields_error(self, mock_cases):
        from process_contact_flow_event import process_task_event, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "Not authorized"}},
            "ListFields",
        )

        with self.assertRaises(ClientError):
            process_task_event(TASK_EVENT)
        print("PASS: re-raises ClientError from list_fields")


class TestSendSnsNotification(unittest.TestCase):
    """Test send_sns_notification function."""

    @patch("process_contact_flow_event._sns_client")
    def test_sends_flat_result_as_sns_body(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        mock_sns.publish.return_value = {"MessageId": "msg-12345"}

        result = {"channel": "TASK", "title": "Test Case", "status": "Open"}
        message_id = send_sns_notification(result, "contact-123", "arn:aws:sns:us-east-1:123:my-topic")

        assert message_id == "msg-12345"
        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args[1]
        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123:my-topic"
        assert "Amazon Connect TASK - Contact contact-123" in call_kwargs["Subject"]
        body = json.loads(call_kwargs["Message"])
        assert body["channel"] == "TASK"
        assert body["title"] == "Test Case"
        assert body["status"] == "Open"
        print("PASS: sends flat result as SNS message body")

    @patch("process_contact_flow_event._sns_client")
    def test_returns_none_when_topic_arn_empty(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        result = {"channel": "TASK"}
        message_id = send_sns_notification(result, "c-1", "")

        assert message_id is None
        mock_sns.publish.assert_not_called()
        print("PASS: returns None and skips publish when topic ARN is empty")

    @patch("process_contact_flow_event._sns_client")
    def test_returns_none_on_publish_error(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        mock_sns.publish.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameter", "Message": "Bad ARN"}},
            "Publish",
        )

        result = {"channel": "TASK"}
        message_id = send_sns_notification(result, "c-2", "arn:aws:sns:us-east-1:123:bad-topic")

        assert message_id is None
        print("PASS: returns None on SNS publish failure")

    @patch("process_contact_flow_event._sns_client")
    def test_truncates_long_subject(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        mock_sns.publish.return_value = {"MessageId": "msg-999"}

        long_contact_id = "a" * 100
        result = {"channel": "TASK"}
        send_sns_notification(result, long_contact_id, "arn:aws:sns:us-east-1:123:topic")

        call_kwargs = mock_sns.publish.call_args[1]
        assert len(call_kwargs["Subject"]) <= 100
        print("PASS: truncates subject to 100 characters")


class TestLambdaHandler(unittest.TestCase):
    """End-to-end tests for lambda_handler."""

    @patch("process_contact_flow_event._sns_client")
    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:topic")
    def test_task_event_end_to_end(self, mock_cases, mock_sns):
        from process_contact_flow_event import lambda_handler, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.return_value = LIST_FIELDS_RESPONSE
        mock_cases.get_case.return_value = GET_CASE_RESPONSE
        mock_sns.publish.return_value = {"MessageId": "msg-abc"}

        result = lambda_handler(TASK_EVENT, None)

        assert result["channel"] == "TASK"
        assert result["sns_message_id"] == "msg-abc"
        # Case fields use human-readable names as keys
        assert result["Title"] == "Incorrect Shipping Address"
        assert result["Status"] == "Open"
        assert result["Reference Number"] == 100123.0
        assert result["Summary"] is None  # emptyValue -> None
        assert result["Priority Level"] == "High"
        assert "arn:aws:connect" in result["Assigned User"]
        # SNS message body is the same flat structure (without sns_message_id)
        mock_sns.publish.assert_called_once()
        sns_body = json.loads(mock_sns.publish.call_args[1]["Message"])
        assert sns_body["channel"] == "TASK"
        assert sns_body["Title"] == "Incorrect Shipping Address"
        assert sns_body["Status"] == "Open"
        assert "sns_message_id" not in sns_body
        mock_cases.list_fields.assert_called_once()
        mock_cases.get_case.assert_called_once()
        print("PASS: TASK event returns field names as keys with None for empty")

    @patch("process_contact_flow_event._sns_client")
    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", None)
    def test_task_event_without_sns(self, mock_cases, mock_sns):
        from process_contact_flow_event import lambda_handler, _field_name_cache
        _field_name_cache.clear()

        mock_cases.list_fields.return_value = LIST_FIELDS_RESPONSE
        mock_cases.get_case.return_value = GET_CASE_RESPONSE

        result = lambda_handler(TASK_EVENT, None)

        assert result["channel"] == "TASK"
        assert result["sns_message_id"] == "not_sent"
        assert result["Title"] == "Incorrect Shipping Address"
        assert result["Status"] == "Open"
        mock_sns.publish.assert_not_called()
        print("PASS: TASK event succeeds even without SNS configured")

    @patch("process_contact_flow_event._CASES_DOMAIN_ID", None)
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", None)
    def test_task_event_without_domain_id(self):
        from process_contact_flow_event import lambda_handler

        result = lambda_handler(TASK_EVENT, None)

        assert result["channel"] == "TASK"
        assert "CASES_DOMAIN_ID" in result["error"]
        print("PASS: TASK event returns error when CASES_DOMAIN_ID not set")


if __name__ == "__main__":
    unittest.main(verbosity=2)
