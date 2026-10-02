class FieldMapping:
    def __init__(self, file_name_field, catalogue_reference_field):
        self.file_name_field = file_name_field
        self.catalogue_reference_field = catalogue_reference_field


class AWSConfig:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name


def field_mapping(source_system):
    return {
        "PA": FieldMapping("file_name", "catalogue_reference"),
        "ADHOC": FieldMapping("fileName", "catRef")
    }[source_system]


def aws_config(environment, source_system):
    return {
        "PA": AWSConfig("pa-transfer-bucket"),
        "ADHOC": AWSConfig(f"{environment}-dr2-ingest-adhoc-cache")
    }[source_system]
