locals {
  ingest_queue_creator_name = "${local.environment}-dr2-custodial-copy-queue-creator"
}

module "dr2_custodial_copy_queue_creator_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.ingest_queue_creator_name
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.ingest_queue_creator_name
    topic_arn  = local.entity_event_topic_arn
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  encryption_type                                   = local.sse_encryption
  visibility_timeout                                = local.visibility_timeout
}

module "dr2_custodial_copy_queue_creator_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name = local.ingest_queue_creator_name
  handler       = "uk.gov.nationalarchives.custodialcopyqueuecreator.Lambda::handleRequest"
  policies = {
    "${local.ingest_queue_creator_name}-policy" = templatefile("${path.module}/templates/iam_policy/custodial_copy_queue_creator_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.ingest_queue_creator_name
      custodial_copy_fifo_queue  = module.dr2_custodial_copy_queue.sqs_arn
      queue_creator_input_queue  = module.dr2_custodial_copy_queue_creator_queue.sqs_arn
      secrets_manager_secret_arn = aws_secretsmanager_secret.preservica_read_metadata.arn
    })
  }
  timeout_seconds = 180
  memory_size     = local.java_lambda_memory_size
  runtime         = local.java_runtime
  tags            = {}
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn = module.dr2_custodial_copy_queue_creator_queue.sqs_arn
  }]
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  plaintext_env_vars = {
    PRESERVICA_SECRET_NAME = aws_secretsmanager_secret.preservica_read_metadata.name
    OUTPUT_QUEUE_URL       = module.dr2_custodial_copy_queue.sqs_queue_url
  }
}
