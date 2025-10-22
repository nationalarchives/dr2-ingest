locals {
  rotate_preservation_system_password_name = "${local.environment}-dr2-rotate-preservation-system-password"
}

module "dr2_rotate_preservation_system_password_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = local.rotate_preservation_system_password_name
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${local.rotate_preservation_system_password_name}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.rotate_preservation_system_password_name}-policy" = templatefile("./templates/iam_policy/rotate_preservation_system_password_policy.json.tpl", {
      secrets_manager_secret_arns = jsonencode([
        aws_secretsmanager_secret.preservica_secret.arn,
        aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.arn,
        aws_secretsmanager_secret.preservica_read_metadata.arn,
        aws_secretsmanager_secret.preservica_read_metadata_read_content.arn
      ]),
      account_id  = data.aws_caller_identity.current.account_id
      lambda_name = local.rotate_preservation_system_password_name
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = local.rotate_preservation_system_password_name
  }
}

resource "aws_lambda_permission" "rotate_secrets_permissions" {
  for_each = toset([aws_secretsmanager_secret.preservica_secret.arn,
    aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.arn,
    aws_secretsmanager_secret.preservica_read_metadata.arn,
  aws_secretsmanager_secret.preservica_read_metadata_read_content.arn])
  action        = "lambda:InvokeFunction"
  function_name = local.rotate_preservation_system_password_name
  principal     = "secretsmanager.amazonaws.com"
  source_arn    = each.key
}