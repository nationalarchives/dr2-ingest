class FieldMapping:
    def __init__(self, file_name_field, catalogue_reference_field):
        self.file_name_field = file_name_field
        self.catalogue_reference_field = catalogue_reference_field


class AWSConfig:
    def __init__(self, bucket_name: str, queue_name):
        self.bucket_name = bucket_name
        self.queue_name = queue_name


def field_mapping(source_system):
    return {
        "PA": FieldMapping("file_name", "catalogue_reference"),
        "ADHOC": FieldMapping("fileName", "catRef")
    }[source_system]


def aws_config(environment):
    pa_bucket_environment = "dev" if environment == "intg" else environment
    return {
        "PA": AWSConfig(f"{pa_bucket_environment}-pa-transfer", f"{environment}-dr2-preingest-pa-importer"),
        "ADHOC": AWSConfig(f"{environment}-dr2-ingest-adhoc-cache", f"{environment}-dr2-preingest-adhoc-importer")
    }
