locals {
  rotate_preservation_system_password_name = "${local.environment}-dr2-rotate-preservation-system-password"
}

module "dr2_rotate_preservation_system_password_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.rotate_preservation_system_password_name
  handler         = "uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.rotate_preservation_system_password_name}-policy" = templatefile("./templates/iam_policy/rotate_preservation_system_password_policy.json.tpl", {
      secrets_manager_secret_arns = jsonencode([
        var.secrets.preservica_secret.arn,
        var.secrets.preservica_read_update_metadata_insert_content.arn,
        var.secrets.preservica_read_metadata.arn,
        var.secrets.preservica_read_metadata_read_content.arn
      ]),
      account_id  = data.aws_caller_identity.current.account_id
      lambda_name = local.rotate_preservation_system_password_name
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = local.rotate_preservation_system_password_name
  }
}

resource "aws_lambda_permission" "rotate_secrets_permissions" {
  for_each = toset([var.secrets.preservica_secret.arn,
    var.secrets.preservica_read_update_metadata_insert_content.arn,
    var.secrets.preservica_read_metadata.arn,
  var.secrets.preservica_read_metadata_read_content.arn])
  action        = "lambda:InvokeFunction"
  function_name = local.rotate_preservation_system_password_name
  principal     = "secretsmanager.amazonaws.com"
  source_arn    = each.key
}