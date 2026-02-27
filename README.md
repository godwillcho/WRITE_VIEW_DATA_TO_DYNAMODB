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

## File Structure

```
├── template.yaml                    # CloudFormation template (all resources inline)
├── extract_view_questions.py        # Standalone ExtractViewQuestions Lambda source
├── test.py                          # Standalone NormalizeViewData Lambda source
├── test_extract_views_event.json    # Sample test event for ExtractViewQuestions
└── README.md
```

> **Note**: The Lambda code is deployed inline via CloudFormation `ZipFile`. The standalone `.py` files are kept in sync for local development and testing.

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
