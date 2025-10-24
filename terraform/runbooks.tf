locals {
  assume_role_policy = templatefile("${path.root}/templates/iam_role/github_assume_role.json.tpl", {
    account_id   = data.aws_caller_identity.current.account_id,
    repo_filters = jsonencode(["repo:nationalarchives/dr2-runbooks:environment:${local.environment}"])
  })
}

module "remove_all_nacl_rules_role" {
  source             = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = local.assume_role_policy
  name               = "${local.environment}-dr2-runbook-remove-all-nacl-rules"
  policy_attachments = {
    remove_all_nacl_rules_policy = module.remove_all_nacl_rules_policy.policy_arn
  }
  tags = {}
}

module "remove_all_nacl_rules_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-runbooks-remove-all-nacl-rules-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/remove_all_nacl_rules.json.tpl", {
    public_nacl_arn  = module.vpc.public_nacl_arn
    private_nacl_arn = module.vpc.private_nacl_arn
  })
}

module "pause_ingest_role" {
  source             = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = local.assume_role_policy
  name               = "${local.environment}-dr2-runbook-pause-ingest"
  policy_attachments = {
    pause_ingest_policy = module.pause_ingest_policy.policy_arn
  }
  tags = {}
}

module "pause_preservica_activity_role" {
  source             = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = local.assume_role_policy
  name               = "${local.environment}-dr2-runbook-pause-preservica-activity"
  policy_attachments = {
    pause_ingest_policy = module.pause_preservica_activity_policy.policy_arn
  }
  tags = {}
}

module "pause_ingest_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-runbooks-pause-ingest-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/runbook_invoke_lambda_policy.json.tpl", {
    lambda_arn = module.pause_ingest_lambda.lambda_arn
  })
}

module "pause_preservica_activity_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-runbooks-pause-preservica-activity-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/runbook_invoke_lambda_policy.json.tpl", {
    lambda_arn = module.pause_preservica_activity_lambda.lambda_arn
  })
}
