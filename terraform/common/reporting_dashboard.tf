module "generate_reporting_data_role" {
  count  = local.environment == "prod" ? 1 : 0
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.root}/templates/iam_role/github_assume_role.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
  repo_filters = jsonencode(["repo:nationalarchives/dr2-reporting:ref:refs/heads/main"]) })
  name = "${local.environment}-dr2-ingest-reporting-role"
  policy_attachments = {
    deploy_policy = module.generate_reporting_data_policy[count.index].policy_arn
  }
  tags = {}
}

module "generate_reporting_data_policy" {
  count  = local.environment == "prod" ? 1 : 0
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-reporting-policy"
  policy_string = templatefile("./templates/iam_policy/ingest_reporting_policy.json.tpl", {
    account_id  = data.aws_caller_identity.current.account_id
    environment = local.environment
  })
}
