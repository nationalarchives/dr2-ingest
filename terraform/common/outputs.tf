variable "environment" {}

output "step_function_names" {
  value = {
    ingest = "${var.environment}-ingest"
    preingest = {
      tdr = "${var.environment}-dr2-preingest-tdr"
      dri = "${var.environment}-dr2-preingest-dri"
    }
  }
}

output "table_names" {
  value = {
    postingest = "${var.environment}-dr2-postingest-state"
  }
}

output "lambda_names" {
  value = {
    ingest_asset_opex_creator  = "${var.environment}-dr2-ingest-asset-opex-creator"
    find_existing_asset        = "${var.environment}-dr2-ingest-find-existing-asset"
    validate_ingest_inputs     = "${var.environment}-dr2-ingest-validate-generic-ingest-inputs"
    ingest_reconciler          = "${var.environment}-dr2-ingest-asset-reconciler"
    parent_folder_opex_creator = "${var.environment}-dr2-ingest-parent-folder-opex-creator"
    start_workflow             = "${var.environment}-dr2-ingest-start-workflow"
    workflow_monitor           = "${var.environment}-dr2-ingest-workflow-monitor"
    preingest = {
      tdr = {
        importer        = "${var.environment}-dr2-preingest-tdr-importer"
        aggregator      = "${var.environment}-dr2-preingest-tdr-aggregator"
        package_builder = "${var.environment}-dr2-preingest-tdr-package-builder"
      }
      dri = {
        importer        = "${var.environment}-dr2-preingest-dri-importer"
        aggregator      = "${var.environment}-dr2-preingest-dri-aggregator"
        package_builder = "${var.environment}-dr2-preingest-dri-package-builder"
      }
    }
    ingest_mapper          = "${var.environment}-dr2-ingest-mapper"
    court_document_handler = "${var.environment}-dr2-ingest-parsed-court-document-event-handler"
    upsert_folders         = "${var.environment}-dr2-ingest-upsert-archive-folders"
    folder_opex_creator    = "${var.environment}-dr2-ingest-folder-opex-creator"
    failed_notification    = "${var.environment}-dr2-ingest-failure-notifications"
  }
}
