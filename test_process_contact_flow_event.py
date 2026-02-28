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

CHAT_EVENT = {
    "Details": {
        "ContactData": {
            "Attributes": {
                "customerName": "John Doe",
                "accountId": "ACC-12345",
                "reason": "Billing inquiry",
            },
            "AwsRegion": "us-east-1",
            "Channel": "CHAT",
            "ContactId": "abc12345-6789-0def-ghij-klmnopqrstuv",
            "CustomerEndpoint": {"Address": "chat-endpoint"},
            "InitialContactId": "abc12345-6789-0def-ghij-klmnopqrstuv",
            "InitiationMethod": "API",
            "InstanceARN": "arn:aws:connect:us-east-1:125677696735:instance/110e2f11-ec6e-44b9-83cb-32dac6722467",
            "Name": "",
            "Queue": {"Name": "BasicQueue"},
            "References": {},
            "RelatedContactId": None,
        },
        "Parameters": {},
    },
    "Name": "ContactFlowEvent",
}

VOICE_EVENT = {
    "Details": {
        "ContactData": {
            "Attributes": {},
            "Channel": "VOICE",
            "ContactId": "voice-contact-id",
            "InstanceARN": "arn:aws:connect:us-east-1:125677696735:instance/110e2f11",
            "References": {},
        },
        "Parameters": {},
    },
    "Name": "ContactFlowEvent",
}

GET_CASE_RESPONSE = {
    "fields": [
        {"id": "title", "value": {"stringValue": "Incorrect Shipping Address"}},
        {"id": "status", "value": {"stringValue": "Open"}},
        {"id": "assigned_user", "value": {"userArnValue": "arn:aws:connect:us-east-1:123:instance/i/agent/a"}},
        {"id": "reference_number", "value": {"doubleValue": 100123.0}},
        {"id": "summary", "value": {"emptyValue": {}}},
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
    def test_processes_task_event_successfully(self, mock_cases):
        from process_contact_flow_event import process_task_event

        mock_cases.get_case.return_value = GET_CASE_RESPONSE

        result = process_task_event(TASK_EVENT)

        assert result["channel"] == "TASK"
        assert result["case_id"] == "de4d7f2d-592d-3b68-908d-d09e5dc93471"
        assert result["contact_id"] == "54af2d58-1c85-4586-8c16-1457620e9920"
        assert result["task_name"] == "Action-Required - Cases de4d7f2d-592d-3b68-908d-d09e5dc93471"
        assert result["fields"]["title"] == "Incorrect Shipping Address"
        assert result["fields"]["status"] == "Open"
        assert result["fields"]["reference_number"] == 100123.0
        assert result["fields"]["summary"] is None  # emptyValue
        assert "arn:aws:connect" in result["fields"]["assigned_user"]  # userArnValue

        mock_cases.get_case.assert_called_once()
        print("PASS: processes TASK event with all field types")

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
    def test_raises_on_api_error(self, mock_cases):
        from process_contact_flow_event import process_task_event

        mock_cases.get_case.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Case not found"}},
            "GetCase",
        )

        with self.assertRaises(ClientError):
            process_task_event(TASK_EVENT)
        print("PASS: re-raises ClientError from Cases API")


class TestProcessChatEvent(unittest.TestCase):
    """Test process_chat_event function."""

    def test_processes_chat_event_successfully(self):
        from process_contact_flow_event import process_chat_event

        result = process_chat_event(CHAT_EVENT)

        assert result["channel"] == "CHAT"
        assert result["contact_id"] == "abc12345-6789-0def-ghij-klmnopqrstuv"
        assert result["fields"]["customerName"] == "John Doe"
        assert result["fields"]["accountId"] == "ACC-12345"
        assert result["fields"]["reason"] == "Billing inquiry"
        assert len(result["fields"]) == 3
        print("PASS: processes CHAT event with all attributes")

    def test_handles_empty_attributes(self):
        from process_contact_flow_event import process_chat_event

        event = {
            "Details": {
                "ContactData": {
                    "Channel": "CHAT",
                    "ContactId": "contact-456",
                    "Attributes": {},
                }
            }
        }
        result = process_chat_event(event)

        assert result["channel"] == "CHAT"
        assert result["contact_id"] == "contact-456"
        assert result["fields"] == {}
        print("PASS: handles CHAT event with empty attributes")


class TestSendSnsNotification(unittest.TestCase):
    """Test send_sns_notification function."""

    @patch("process_contact_flow_event._sns_client")
    def test_sends_notification_successfully(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        mock_sns.publish.return_value = {"MessageId": "msg-12345"}

        data = {"channel": "TASK", "contact_id": "contact-123", "fields": {"title": "Test"}}
        message_id = send_sns_notification(data, "arn:aws:sns:us-east-1:123:my-topic")

        assert message_id == "msg-12345"
        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args[1]
        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123:my-topic"
        assert "Amazon Connect TASK - Contact contact-123" in call_kwargs["Subject"]
        body = json.loads(call_kwargs["Message"])
        assert body["fields"]["title"] == "Test"
        print("PASS: sends SNS notification with correct subject and JSON body")

    @patch("process_contact_flow_event._sns_client")
    def test_returns_none_when_topic_arn_empty(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        data = {"channel": "CHAT", "contact_id": "c-1", "fields": {}}
        message_id = send_sns_notification(data, "")

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

        data = {"channel": "TASK", "contact_id": "c-2", "fields": {}}
        message_id = send_sns_notification(data, "arn:aws:sns:us-east-1:123:bad-topic")

        assert message_id is None
        print("PASS: returns None on SNS publish failure")

    @patch("process_contact_flow_event._sns_client")
    def test_truncates_long_subject(self, mock_sns):
        from process_contact_flow_event import send_sns_notification

        mock_sns.publish.return_value = {"MessageId": "msg-999"}

        long_contact_id = "a" * 100
        data = {"channel": "TASK", "contact_id": long_contact_id, "fields": {}}
        send_sns_notification(data, "arn:aws:sns:us-east-1:123:topic")

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
        from process_contact_flow_event import lambda_handler

        mock_cases.get_case.return_value = GET_CASE_RESPONSE
        mock_sns.publish.return_value = {"MessageId": "msg-abc"}

        result = lambda_handler(TASK_EVENT, None)

        assert result["status"] == "success"
        assert result["channel"] == "TASK"
        assert result["case_id"] == "de4d7f2d-592d-3b68-908d-d09e5dc93471"
        assert result["sns_message_id"] == "msg-abc"
        # Case fields are prefixed with "case_" to avoid key collisions
        assert result["case_title"] == "Incorrect Shipping Address"
        assert result["case_status"] == "Open"
        assert result["case_reference_number"] == "100123.0"
        assert result["case_summary"] == ""  # emptyValue -> empty string
        mock_cases.get_case.assert_called_once()
        mock_sns.publish.assert_called_once()
        print("PASS: TASK event end-to-end with SNS notification")

    @patch("process_contact_flow_event._sns_client")
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:topic")
    def test_chat_event_end_to_end(self, mock_sns):
        from process_contact_flow_event import lambda_handler

        mock_sns.publish.return_value = {"MessageId": "msg-def"}

        result = lambda_handler(CHAT_EVENT, None)

        assert result["status"] == "success"
        assert result["channel"] == "CHAT"
        # Chat fields are prefixed with "chat_"
        assert result["chat_customerName"] == "John Doe"
        assert result["chat_accountId"] == "ACC-12345"
        assert result["chat_reason"] == "Billing inquiry"
        assert result["sns_message_id"] == "msg-def"
        mock_sns.publish.assert_called_once()
        print("PASS: CHAT event end-to-end with SNS notification")

    @patch("process_contact_flow_event._SNS_TOPIC_ARN", None)
    def test_unsupported_channel_returns_error(self):
        from process_contact_flow_event import lambda_handler

        result = lambda_handler(VOICE_EVENT, None)

        assert result["status"] == "error"
        assert "Unsupported channel" in result["error"]
        assert result["contact_id"] == "voice-contact-id"
        print("PASS: unsupported channel returns error response")

    @patch("process_contact_flow_event._sns_client")
    @patch("process_contact_flow_event._cases_client")
    @patch("process_contact_flow_event._CASES_DOMAIN_ID", "domain-123")
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", None)
    def test_task_event_without_sns(self, mock_cases, mock_sns):
        from process_contact_flow_event import lambda_handler

        mock_cases.get_case.return_value = GET_CASE_RESPONSE

        result = lambda_handler(TASK_EVENT, None)

        assert result["status"] == "success"
        assert result["sns_message_id"] == "not_sent"
        mock_sns.publish.assert_not_called()
        print("PASS: TASK event succeeds even without SNS configured")

    @patch("process_contact_flow_event._CASES_DOMAIN_ID", None)
    @patch("process_contact_flow_event._SNS_TOPIC_ARN", None)
    def test_task_event_without_domain_id(self):
        from process_contact_flow_event import lambda_handler

        result = lambda_handler(TASK_EVENT, None)

        assert result["status"] == "error"
        assert "CASES_DOMAIN_ID" in result["error"]
        print("PASS: TASK event returns error when CASES_DOMAIN_ID not set")


if __name__ == "__main__":
    unittest.main(verbosity=2)
