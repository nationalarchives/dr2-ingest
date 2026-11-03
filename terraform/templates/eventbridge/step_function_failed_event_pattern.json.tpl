{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "status": ["FAILED", "TIMED_OUT", "ABORTED"],
    "stateMachineArn": ${step_function_arns},
    "executionArn": [{
      "anything-but": {
        "wildcard": "arn:aws:states:eu-west-2:${account_id}:execution:${environment}-dr2-*:E2E*"
      }
    }]
  }
}
