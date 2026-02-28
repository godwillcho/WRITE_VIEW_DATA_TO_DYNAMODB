# Amazon Connect View Data to DynamoDB

A CloudFormation stack that captures Amazon Connect step-by-step guide (View) responses, enriches them with contact metadata, and stores them in DynamoDB for reporting.

## Architecture

The stack deploys two Lambda functions and two DynamoDB tables:

```
Amazon Connect Flow
        │
        ▼
┌──────────────────────┐       ┌──────────────────────┐
│ NormalizeViewData     │──────▶│ ConnectViewData       │
│ Lambda                │ write │ DynamoDB Table        │
│                       │       │ PK: InitialContactId  │
│ - Normalize multi-    │       │ TTL: 1 year           │
│   select answers      │       └──────────────────────┘
│ - Enrich with agent,  │              ▲
│   timestamps, campaign│              │ query for
│ - Lookup question text │              │ question text
│   from ViewQuestions  │       ┌──────┴───────────────┐
└──────────────────────┘       │ ViewQuestions          │
                                │ DynamoDB Table        │
┌──────────────────────┐       │ PK: Name  SK: Label   │
│ ExtractViewQuestions  │──────▶│                       │
│ Lambda                │ write └───────────────────────┘
│                       │
│ - Describe all Views  │   ◄── EventBridge Scheduler (every 30 days)
│ - Extract Name+Label  │   ◄── Custom Resource (on every deploy)
│   pairs (questions)   │
└──────────────────────┘
```

## What It Does

### 1. NormalizeViewData Lambda

Invoked by Amazon Connect flows after a step-by-step guide is submitted. It:

- **Normalizes multi-select answers**: Converts `{"0":"A","1":"B"}` into `"A, B"` for use in Connect contact attributes.
- **Writes enriched records to DynamoDB** with:
  - Contact IDs (ContactId, PreviousContactId, RelatedContactId)
  - Customer/system endpoints and outbound caller ID
  - Flow metadata (InitiationMethod, Channel, QueueName)
  - Agent details (AgentId, AgentName, AgentUsername) via `describe_contact` + `describe_user`
  - Timestamps (Initiation, Disconnect, ConnectedToAgent, Enqueue, etc.)
  - Campaign info (CampaignId, CampaignName) via `describe_campaign`
  - Question text for each answer key (e.g., `REDE_Q1_Question`) looked up from the ViewQuestions table
  - CreatedAt timestamp in the configured timezone
  - 1-year TTL for automatic cleanup
- **Follows the RelatedContactId chain** (up to 5 hops) to find the original VOICE contact for accurate agent/timestamp data.

### 2. ExtractViewQuestions Lambda

Extracts question labels from Amazon Connect View templates:

- Calls `list_views` to auto-discover all Views in the Connect instance.
- Calls `describe_view` for each View and parses the template JSON.
- Recursively extracts `Name` + `Label` pairs where the component has its own `Name` key (questions only, not option labels).
- Writes each pair to the ViewQuestions DynamoDB table (PK=Name, SK=Label).
- Runs automatically:
  - **On every CloudFormation deployment** via a Custom Resource.
  - **Every 30 days** via EventBridge Scheduler.

## CloudFormation Resources (13 total)

| Resource | Type | Purpose |
|----------|------|---------|
| `ViewDataTable` | DynamoDB::Table | Contact data (PK: InitialContactId, TTL enabled) |
| `ViewQuestionsTable` | DynamoDB::Table | View question labels (PK: Name, SK: Label) |
| `LambdaExecutionRole` | IAM::Role | Role for NormalizeViewData Lambda |
| `NormalizeViewDataFunction` | Lambda::Function | Normalize + enrich + write contact data |
| `ExtractViewQuestionsRole` | IAM::Role | Role for ExtractViewQuestions Lambda |
| `ExtractViewQuestionsFunction` | Lambda::Function | Extract questions from Views |
| `ConnectInvokePermission` | Lambda::Permission | Allow Connect to invoke NormalizeViewData |
| `ConnectLambdaAssociation` | Connect::IntegrationAssociation | Register Lambda with Connect instance |
| `ExtractViewQuestionsSchedulerRole` | IAM::Role | Role for EventBridge Scheduler |
| `ExtractViewQuestionsSchedule` | Scheduler::Schedule | Periodic View extraction (30 days) |
| `TriggerExtractRole` | IAM::Role | Role for Custom Resource trigger Lambda |
| `TriggerExtractFunction` | Lambda::Function | Custom Resource provider |
| `TriggerExtractViewQuestions` | Custom::TriggerExtract | Triggers extraction on deploy |

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ConnectInstanceArn` | *(required)* | ARN of the Amazon Connect instance |
| `DynamoDBTableName` | `ConnectViewData` | Name of the main DynamoDB table |
| `PartitionKeyName` | `InitialContactId` | Partition key attribute name |
| `TZOffsetHours` | `-5` | UTC offset for CreatedAt timestamps |
| `TZLabel` | `EST` | Label shown in timestamps |
| `LogLevel` | `INFO` | Lambda log level (DEBUG/INFO/WARNING/ERROR) |
| `ScheduleExpression` | `rate(30 days)` | EventBridge Scheduler expression |

## Label Overrides

The `ExtractViewQuestions` Lambda supports overriding the label for specific `Name` values. Instead of using the label extracted from the View template, you can specify a custom label in the `LABEL_OVERRIDES` dictionary.

Edit the dictionary in both `extract_view_questions.py` and the inline copy in `template.yaml`:

```python
LABEL_OVERRIDES: Dict[str, str] = {
    "WelcomeGuide_Q4_Yes": "Accepted - Welcome Guide Q4",
    "WelcomeGuide_Q4_No": "Declined - Welcome Guide Q4",
}
```

Names not in the dictionary keep their original labels from the View template.

## Deployment

### Prerequisites

- AWS CLI installed and configured with appropriate credentials
- An existing Amazon Connect instance

### Deploy the stack

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name write-amazon-connect-view-data-to-dynamodb \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ConnectInstanceArn="arn:aws:connect:us-east-1:123456789012:instance/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" \
    DynamoDBTableName="ConnectViewData" \
    PartitionKeyName="InitialContactId" \
    TZOffsetHours=-5 \
    TZLabel="EST" \
    LogLevel="INFO" \
    ScheduleExpression="rate(30 days)"
```

Replace `ConnectInstanceArn` with your actual Connect instance ARN. All other parameters have defaults and are optional.

### What happens on deploy

1. Two DynamoDB tables are created (`ViewDataTable` + `ViewQuestionsTable`).
2. Two Lambdas are created (`NormalizeViewData` + `ExtractViewQuestions`).
3. The `NormalizeViewData` Lambda is automatically associated with your Connect instance.
4. The `ExtractViewQuestions` Lambda runs immediately via the Custom Resource (populates the questions table).
5. An EventBridge schedule is created to re-run extraction every 30 days.

### Get stack outputs

```bash
aws cloudformation describe-stacks \
  --stack-name write-amazon-connect-view-data-to-dynamodb \
  --query "Stacks[0].Outputs"
```

### Use in a Connect flow

After deployment, the `NormalizeViewData` Lambda is automatically registered with your Connect instance. In your contact flow:

1. Add an **Invoke AWS Lambda function** block after your step-by-step guide.
2. Select the `ConnectViewData-NormalizeViewData` function.
3. Pass `viewResultData` and optionally `viewAction` as parameters.
4. The Lambda returns normalized attributes as key-value pairs usable as contact attributes.

## Utility Scripts

### Enable Case Event Streams for Contact Lens Rules

`enable_case_event_streams.py` follows the [AWS documentation](https://docs.aws.amazon.com/connect/latest/adminguide/cases-rules-integration-onboarding.html) to allow Amazon Connect Cases to send updates to Contact Lens rules. It performs three steps:

1. **Enable case event streams** — configures Cases to publish events to EventBridge via `put_case_event_configuration`
2. **Create an event integration** — creates an AppIntegrations event integration for `aws.cases` events
3. **Associate the integration** — links the event integration to your Connect instance so Contact Lens rules can use `OnCaseCreate`, `OnCaseUpdate`, and `OnSlaBreach` triggers

#### Usage

```bash
# Full setup (all 3 steps)
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
```

#### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--instance-id` | *(required)* | Amazon Connect instance ID |
| `--domain-id` | *(required)* | Amazon Connect Cases domain ID |
| `--region` | `us-west-2` | AWS region |
| `--extra-fields` | none | Additional case field IDs to include in events |
| `--list-fields` | off | List available fields and exit |
| `--create-sqs-route` | off | Also create EventBridge → SQS route (not needed for Contact Lens rules) |
| `--integration-name` | `amazon-connect-cases` | Custom name for the event integration |

#### Required IAM Permissions

- `cases:PutCaseEventConfiguration`, `cases:GetCaseEventConfiguration`, `cases:ListFields`
- `app-integrations:CreateEventIntegration`, `app-integrations:GetEventIntegration`
- `connect:CreateIntegrationAssociation`, `connect:ListIntegrationAssociations`
- (Optional for SQS) `sqs:CreateQueue`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, `events:PutRule`, `events:PutTargets`, `sts:GetCallerIdentity`

### Process Contact Flow Event Lambda (TASK / CHAT)

`process_contact_flow_event.py` is a standalone Lambda function invoked from an Amazon Connect contact flow. It routes based on the contact channel:

- **TASK**: Extracts the Case ID from the `taskRef` URL in the event, calls the Cases API (`connectcases:GetCase`) to retrieve case field values, and sends an SNS email notification with all data as JSON.
- **CHAT**: Extracts contact attributes from the event and sends an SNS email notification with all attributes as JSON.

#### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CASES_DOMAIN_ID` | Yes (for TASK) | Amazon Connect Cases domain ID |
| `SNS_TOPIC_ARN` | Yes (for notifications) | ARN of the SNS topic for email notifications |
| `LOG_LEVEL` | No (default: `INFO`) | Logging level (DEBUG/INFO/WARNING/ERROR) |

#### Custom Case Fields

To include custom case fields in TASK event processing, add field IDs to the `CUSTOM_CASE_FIELD_IDS` list in the script:

```python
CUSTOM_CASE_FIELD_IDS: List[str] = [
    "my_custom_field_1",
    "my_custom_field_2",
]
```

Find available field IDs by running:

```bash
python enable_case_event_streams.py --list-fields --instance-id YOUR_ID --domain-id YOUR_DOMAIN
```

#### Return Value

The Lambda returns a flat dict usable as Connect contact attributes. Case fields are prefixed with `case_` and chat attributes with `chat_` to avoid key collisions:

```json
{
  "status": "success",
  "channel": "TASK",
  "contact_id": "54af2d58-...",
  "case_id": "de4d7f2d-...",
  "sns_message_id": "msg-12345",
  "case_title": "Incorrect Shipping Address",
  "case_status": "Open",
  "case_reference_number": "100123.0"
}
```

#### Required IAM Permissions

- `cases:GetCase` on the Cases domain
- `sns:Publish` on the SNS topic
- `AWSLambdaBasicExecutionRole` for CloudWatch Logs

### Update DynamoDB Table Attributes

`update_table_attributes.py` is an ad-hoc CLI script for bulk-updating specific attribute values in a DynamoDB table.

#### Usage

```bash
# Dry run (default — no changes applied)
python update_table_attributes.py

# Apply changes
python update_table_attributes.py --apply

# Specify table, partition key, and region
python update_table_attributes.py --table MyTable --pk MyPartitionKey --region us-east-1
```

Edit the `ATTRIBUTE_OVERRIDES` dictionary in the script to define which attributes to update:

```python
ATTRIBUTE_OVERRIDES: Dict[str, str] = {
    "WelcomeGuide_Q4_Yes": "Your custom label here",
    "WelcomeGuide_Q4_No": "Another custom label",
}
```

| Flag | Default | Description |
|------|---------|-------------|
| `--table` | `ConnectViewData` | DynamoDB table name |
| `--pk` | `InitialContactId` | Partition key name |
| `--region` | `us-west-2` | AWS region |
| `--apply` | off | Apply changes (without this flag, runs in dry-run mode) |

## File Structure

```
├── template.yaml                            # CloudFormation template (all resources inline)
├── extract_view_questions.py                # Standalone ExtractViewQuestions Lambda source
├── test.py                                  # Standalone NormalizeViewData Lambda source
├── process_contact_flow_event.py            # TASK/CHAT contact flow Lambda (Cases + SNS)
├── test_process_contact_flow_event.py       # Tests for process_contact_flow_event.py
├── enable_case_event_streams.py             # Enable case event streams for Contact Lens rules
├── test_enable_case_event_streams.py        # Tests for enable_case_event_streams.py
├── update_table_attributes.py               # Ad-hoc bulk attribute updater for DynamoDB
├── test_extract_views_event.json            # Sample test event for ExtractViewQuestions
└── README.md
```

> **Note**: The Lambda code for NormalizeViewData and ExtractViewQuestions is deployed inline via CloudFormation `ZipFile`. The standalone `.py` files are kept in sync for local development and testing. The utility scripts (`enable_case_event_streams.py`, `update_table_attributes.py`) and the contact flow Lambda (`process_contact_flow_event.py`) are standalone files not part of the CloudFormation stack.

## Testing

### Test ExtractViewQuestions locally

```bash
aws lambda invoke \
  --function-name ConnectViewData-ExtractViewQuestions \
  --payload '{"InstanceId": "your-instance-id"}' \
  --region us-west-2 \
  output.json && cat output.json
```

### Test with specific View ARNs

```bash
aws lambda invoke \
  --function-name ConnectViewData-ExtractViewQuestions \
  --payload file://test_extract_views_event.json \
  --region us-west-2 \
  output.json && cat output.json
```

### Test enable_case_event_streams.py

```bash
python -m pytest test_enable_case_event_streams.py -v
```

### Test process_contact_flow_event.py

```bash
python -m pytest test_process_contact_flow_event.py -v
```
