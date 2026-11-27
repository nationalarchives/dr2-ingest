resource "terraform_data" "create_ingest_sfn_lambda_alias" {
  for_each = var.lambdas
  triggers_replace = [
    each.value
  ]

  provisioner "local-exec" {
    command = <<-EOT
      aws lambda update-alias --function-name ${each.key} --name ${var.alias_name} --function-version ${each.value} || \
      aws lambda create-alias --function-name ${each.key} --name ${var.alias_name} --function-version ${each.value}
    EOT
  }
}