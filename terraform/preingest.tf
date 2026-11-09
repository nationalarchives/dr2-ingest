locals {
  object_store_bucket_name = "prod-daobjectstore"
  pa_source_bucket         = local.environment == "intg" ? "dev-pa-transfer" : "${local.environment}-pa-transfer"
  preingest_sources = {
    tdr = {
      sns_topic_subscription = {
        topic_arn = "arn:aws:sns:eu-west-2:${module.tdr_config.account_numbers[local.environment]}:tdr-external-notifications-${local.environment}"
        filter_policy = templatefile("${path.module}/templates/sns/tdr_preingest_filter_policy.json.tpl", {
          bucket = local.tdr_export_bucket
        })
      }
      source_name            = lower(local.tdr)
      bucket_kms_arn         = module.tdr_config.terraform_config["${local.environment}_s3_export_bucket_kms_key_arn"]
      copy_source_bucket_arn = "arn:aws:s3:::${local.tdr_export_bucket}"
    }


    pa = {
      source_name            = "pa"
      copy_source_bucket_arn = "arn:aws:s3:::${local.pa_source_bucket}"
      importer_lambda = {
        visibility_timeout = 900
        timeout            = 900
        handler            = "uk.gov.nationalarchives.preingestpaimporter.Lambda::handleRequest"
        runtime            = local.java_runtime
        memory_size        = 2048
        architecture       = local.architecture_arm64
      }
    }

    dri = {
      source_name            = lower(local.dri)
      copy_source_bucket_arn = "arn:aws:s3:::${local.dri_migration_bucket_name}"
      delete_from_source     = true
      additional_importer_lambda_policies = local.environment == "prod" ? {
        "${local.environment}-copy-from-records-metadata" = templatefile("${path.module}/templates/iam_policy/preingest_dri_records_metadata.json.tpl", {
          records_metadata_bucket = local.records_metadata_bucket_name
          source_bucket           = local.dri_migration_bucket_name
          object_store_bucket     = local.object_store_bucket_name
        })
      } : {}
      additional_importer_lambda_env_vars          = local.environment == "prod" ? { RECORDS_METADATA_BUCKET = local.records_metadata_bucket_name } : {}
      aggregator_secondary_grouping_window_seconds = 600
    }

    ad_hoc = {
      source_name                                  = lower(local.adhoc)
      copy_source_bucket_arn                       = "arn:aws:s3:::${local.adhoc_bucket_name}"
      aggregator_secondary_grouping_window_seconds = 600
      delete_from_source                           = true
    }

    court_document = {
      sns_topic_subscription = local.environment == "prod" ? {
        topic_arn     = module.tre_config.terraform_config[local.tre_environment_name]["da_eventbus"],
        filter_policy = templatefile("${path.module}/templates/sns/tre_live_stream_filter_policy.json.tpl", {})
      } : null
      source_name            = "courtdoc"
      bucket_kms_arn         = module.tre_config.terraform_config["${local.tre_environment_name}_s3_common_kms_arn"]
      copy_source_bucket_arn = module.tre_config.terraform_config[local.tre_environment_name]["s3_common_bucket_arn"]
      importer_lambda = {
        visibility_timeout = 900
        timeout            = 900
        handler            = "uk.gov.nationalarchives.preingestcourtdocimporter.Lambda::handleRequest"
        runtime            = local.java_runtime
        architecture       = local.architecture_arm64
        memory_size        = 512
      }
      package_builder_lambda = {
        handler = "uk.gov.nationalarchives.preingestcourtdocpackagebuilder.Lambda::handleRequest"
      }
    }

    cc_restore = {
      source_name            = "restore"
      copy_source_bucket_arn = "arn:aws:s3:::${local.cc_restore_bucket_name}"
      delete_from_source     = true
      additional_importer_lambda_env_vars = {
        SKIP_VALIDATION = "true"
      }
      package_builder_lambda = {
        handler = "uk.gov.nationalarchives.preingestrestorepackagebuilder.Lambda::handleRequest"
      }
    }
  }
}

module "preingest" {
  source   = "./preingest"
  for_each = local.preingest_sources

  additional_importer_lambda_policies          = try(each.value.additional_importer_lambda_policies, {})
  additional_importer_lambda_env_vars          = try(each.value.additional_importer_lambda_env_vars, {})
  aggregator_secondary_grouping_window_seconds = try(each.value.aggregator_secondary_grouping_window_seconds, 180)
  bucket_kms_arn                               = try(each.value.bucket_kms_arn, null)
  code_deploy_bucket                           = "mgmt-dp-code-deploy"
  copy_source_bucket_arn                       = each.value.copy_source_bucket_arn
  delete_from_source                           = try(each.value.delete_from_source, false)
  environment                                  = local.environment
  general_notifications_channel_id             = local.general_notifications_channel_id
  importer_lambda = try(each.value.importer_lambda, {
    timeout            = 180
    visibility_timeout = 300
    handler            = "lambda_function.lambda_handler"
    runtime            = "python3.12"
    architecture       = "arm64"
    memory_size        = 128
  })
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  lambda_code_version                 = var.lambda_code_version
  notifications_topic_arn             = module.dr2_notifications_sns.sns_arn
  package_builder_lambda = try(each.value.package_builder_lambda, {
    handler = "uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda::handleRequest"
  })
  private_security_group_ids = [
    module.outbound_https_access_for_s3.security_group_id,
    module.https_to_vpc_endpoints_security_group.security_group_id,
    module.outbound_https_access_for_dynamo_db.security_group_id
  ]
  private_subnet_ids        = module.vpc.private_subnets
  slack_api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  sns_topic_subscription    = try(each.value.sns_topic_subscription, null)
  source_name               = each.value.source_name
  vpc_arn                   = module.vpc.vpc.arn
  vpc_id                    = module.vpc.vpc.id
}

moved {
  from = module.tdr_preingest
  to   = module.preingest["tdr"]
}

moved {
  from = module.pa_preingest
  to   = module.preingest["pa"]
}

moved {
  from = module.dri_preingest
  to   = module.preingest["dri"]
}

moved {
  from = module.ad_hoc_preingest
  to   = module.preingest["ad_hoc"]
}

moved {
  from = module.court_document_preingest
  to   = module.preingest["court_document"]
}

moved {
  from = module.cc_restore_preingest
  to   = module.preingest["cc_restore"]
}

