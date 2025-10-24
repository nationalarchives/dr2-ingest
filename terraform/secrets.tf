resource "random_string" "preservica_user" {
  length  = 10
  special = false
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation" {
  for_each = toset([
    aws_secretsmanager_secret.preservica_secret.id,
    aws_secretsmanager_secret.preservica_read_metadata_read_content.id,
    aws_secretsmanager_secret.preservica_read_metadata.id,
    aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.id
  ])
  rotation_lambda_arn = module.ingest[local.environment].rotate_secret_lambda_arn
  secret_id           = each.key
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret" "preservica_secret" {
  name = "${local.environment}-preservica-api-login-details-${random_string.preservica_user.result}"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata_read_content" {
  name = "${local.environment}-preservica-api-read-metadata-read-content"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata" {
  name = "${local.environment}-preservica-api-read-metadata"
}

resource "aws_secretsmanager_secret" "preservica_read_update_metadata_insert_content" {
  name = "${local.environment}-preservica-api-read-update-metadata-insert-content"
}

resource "aws_secretsmanager_secret" "demo_preservica_secret" {
  name = "${local.environment}-demo-preservica-api-login-details-${random_string.preservica_user.result}"
}