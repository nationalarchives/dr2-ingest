{
  "widgets": [
    {
      "height": 6,
      "width": 24,
      "y": 0,
      "x": 0,
      "type": "log",
      "properties": {
        "query": "SOURCE '/aws/lambda/${environment}-ingest-parsed-court-document-event-handler' | SOURCE '/aws/lambda/${environment}-ingest-mapper' | SOURCE '/aws/lambda/${environment}-court-document-package-anonymiser' | SOURCE '/aws/lambda/${environment}-ingest-asset-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-folder-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-parent-folder-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-start-workflow' | SOURCE '/aws/lambda/${environment}-ingest-upsert-archive-folders' | SOURCE '/aws/lambda/${environment}-s3-copy' | fields @timestamp, message, error.message, log.level, @logStream\n| filter log.level == \"ERROR\"\n| sort @timestamp, batchRef desc\n| limit 20",
        "region": "eu-west-2",
        "stacked": false,
        "title": "Errors",
        "view": "table"
      }
    },
    {
      "height": 6,
      "width": 24,
      "y": 6,
      "x": 0,
      "type": "log",
      "properties": {
        "query": "SOURCE '/aws/lambda/${environment}-ingest-parsed-court-document-event-handler' | SOURCE '/aws/lambda/${environment}-ingest-mapper' | SOURCE '/aws/lambda/${environment}-ingest-folder-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-asset-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-parent-folder-opex-creator' | SOURCE '/aws/lambda/${environment}-ingest-start-workflow' | SOURCE '/aws/lambda/${environment}-ingest-upsert-archive-folders' | SOURCE '/aws/lambda/${environment}-s3-copy' | fields @timestamp, batchRef, fileReference, log.logger, message, @logStream\n| filter ispresent(batchRef)\n| sort @timestamp asc",
        "region": "eu-west-2",
        "stacked": false,
        "title": "Lambda Logs",
        "view": "table"
      }
    },
    {
      "height": 2,
      "width": 18,
      "y": 12,
      "x": 0,
      "type": "alarm",
      "properties": {
        "title": "SQS DLQ Alarms",
        "alarms": [
          "arn:aws:cloudwatch:eu-west-2:${account_id}:alarm:${environment}-ingest-parsed-court-document-event-handler-dlq-alarm"
        ]
      }
    },
    {
      "height": 6,
      "width": 24,
      "y": 14,
      "x": 0,
      "type": "log",
      "properties": {
        "query": "SOURCE '/aws/events/${step_function_failure_log_group}' | fields @timestamp, @message, @logStream\n| sort @timestamp desc\n| limit 20",
        "region": "eu-west-2",
        "stacked": false,
        "view": "table"
      }
    }
  ]
}
