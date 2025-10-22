locals {
  repositories = ["dr2-ingest", "dr2-ip-lock-checker", "dr2-ingest-cc-notification-handler"]
  all_repository_filters = flatten([
    for repository in local.repositories : [
      "repo:nationalarchives/${repository}:environment:${local.environment}",
      "repo:nationalarchives/${repository}:ref:refs/heads/main"
    ]
  ])
}
module "deploy_lambda_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.module}/templates/iam_role/github_assume_role.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
  repo_filters = jsonencode(local.all_repository_filters) })
  name = "${local.environment_title}DPGithubActionsDeployLambdaRole"
  policy_attachments = {
    deploy_policy = module.deploy_lambda_policy.policy_arn
  }
  tags = {}
}

module "deploy_lambda_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment_title}DPGithubActionsDeployLambdaPolicy"
  policy_string = templatefile("${path.module}/templates/iam_policy/deploy_lambda_policy.json.tpl", {
    lambda_arns = jsonencode(flatten(
      [
        module.dr2_ip_lock_checker_lambda.lambda_arn,
        module.dr2_custodial_copy_queue_creator_lambda.lambda_arn,
        module.dr2_entity_event_generator_lambda.lambda_arn,
        module.dr2_get_latest_preservica_version_lambda.lambda_arn,
        module.dr2_rotate_preservation_system_password_lambda.lambda_arn,
        module.pause_ingest_lambda.lambda_arn,
        module.pause_preservica_activity_lambda.lambda_arn,
        local.anonymiser_lambda_arns,
      ]
    ))
    bucket_name = "mgmt-dp-code-deploy",
    account_id  = data.aws_caller_identity.current.account_id
  })
}
