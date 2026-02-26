{
  "Statement": [
    {
      "Sid": "sfnPolicy",
      "Effect": "Allow",
      "Action": [
        "states:RedriveExecution",
        "lambda:InvokeFunction",
        "states:StartExecution",
        "events:PutEvents",
        "events:DescribeRule",
        "events:PutRule",
        "events:PutTargets",
        "dynamodb:Query",
        "dynamodb:DeleteItem"
      ],
      "Resource": [
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_validate_generic_ingest_inputs_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_mapper_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_upsert_archive_folders_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_find_existing_asset_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_asset_opex_creator_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_folder_opex_creator_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_parent_folder_opex_creator_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_start_workflow_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_workflow_monitor_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_asset_reconciler_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_flow_control_lambda_name}",
        "arn:aws:dynamodb:eu-west-2:${account_id}:table/${ingest_lock_table_name}",
        "arn:aws:dynamodb:eu-west-2:${account_id}:table/${ingest_lock_table_name}/index/${ingest_lock_table_group_id_gsi_name}",
        "arn:aws:dynamodb:eu-west-2:${account_id}:table/${ingest_queue_table_name}",
        "arn:aws:states:eu-west-2:${account_id}:stateMachine:${ingest_sfn_name}",
        "arn:aws:sns:eu-west-2:${account_id}:${notifications_topic_name}",
        "arn:aws:states:eu-west-2:${account_id}:execution:intg-dr2-ingest/MapOverEachAssetIdAndReconcile:*",
        "arn:aws:states:eu-west-2:${account_id}:stateMachine:${ingest_run_workflow_sfn_name}",
        "arn:aws:states:eu-west-2:${account_id}:execution:${ingest_sfn_name}:*",
        "arn:aws:states:eu-west-2:${account_id}:execution:${ingest_run_workflow_sfn_name}:*",
        "arn:aws:states:eu-west-2:${account_id}:execution:${ingest_sfn_name}/MapOverEachFolderId:*",
        "arn:aws:states:eu-west-2:${account_id}:execution:${ingest_sfn_name}/MapOverEachAssetId:*",
        "arn:aws:events:eu-west-2:${account_id}:event-bus/default",
        "arn:aws:events:eu-west-2:${account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule",
        "${tna_to_preservica_role_arn}"
      ]
    },
    {
      "Sid": "insertPostIngestTable",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:eu-west-2:${account_id}:table/${postingest_table_name}"
      ]
    },
    {
      "Sid": "callPreingestAndRunWorkflowStepFunction",
      "Effect": "Allow",
      "Action": [
        "states:StartExecution"
      ],
      "Resource": [
        "${preingest_dri_step_function_arn}",
        "${preingest_tdr_step_function_arn}",
        "${ingest_run_workflow_sfn_arn}"
      ]
    },
    {
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${ingest_state_bucket_name}",
        "arn:aws:s3:::${ingest_state_bucket_name}/*"
      ],
      "Sid": "readWriteIngestState"
    },
    {
      "Action": [
        "s3:DeleteObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${ingest_state_bucket_name}/*",
        "arn:aws:s3:::${ingest_raw_cache_bucket_name}/*"
      ],
      "Sid": "cleanupIngestFiles"
    }
  ],
  "Version": "2012-10-17"
}
