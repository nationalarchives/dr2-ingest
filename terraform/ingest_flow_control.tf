locals {
  ingest_flow_control_lambda_name = "${local.environment}-dr2-ingest-flow-control"
}

module "dr2_ingest_flow_control_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_flow_control_lambda_name
  handler         = "uk.gov.nationalarchives.ingestflowcontrol.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_flow_control_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_flow_control_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.ingest_flow_control_lambda_name
      dynamo_db_queue_table_arn  = module.ingest_queue_table.table_arn
      ssm_parameter_arn          = aws_ssm_parameter.flow_control_config.arn
      ingest_step_function_arn   = module.dr2_ingest_step_function.step_function_arn
      workflow_step_function_arn = module.dr2_ingest_run_workflow_step_function.step_function_arn
    })
  }

  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    QUEUE_DDB_TABLE   = local.ingest_queue_dynamo_table_name
    CONFIG_PARAM_NAME = local.ingest_flow_control_config_ssm_parameter_name
    INGEST_SFN_ARN    = module.dr2_ingest_run_workflow_step_function.step_function_arn
  }
  tags = {
    Name = local.ingest_flow_control_lambda_name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id]
  }
}