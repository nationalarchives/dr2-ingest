locals {
  ingest_lock_table_group_id_gsi_name = "IngestLockGroupIdx"
  ingest_lock_table_hash_key          = "assetId"
  ingest_lock_dynamo_table_name       = "${local.environment}-dr2-ingest-lock"
}

module "ingest_lock_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = local.ingest_lock_table_hash_key, type = "S" }
  table_name                     = local.ingest_lock_dynamo_table_name
  server_side_encryption_enabled = false
  additional_attributes = [
    { name = "groupId", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.ingest_lock_table_group_id_gsi_name
      hash_key        = "groupId"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = true
}