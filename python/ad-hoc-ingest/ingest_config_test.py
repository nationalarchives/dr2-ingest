from unittest import TestCase
from ingest_config import field_mapping, aws_config


class Test(TestCase):
    def test_field_mapping_should_return_pa_fields(self):
        mapping = field_mapping("PA")

        self.assertEqual("file_name", mapping.file_name_field)
        self.assertEqual("catalogue_reference", mapping.catalogue_reference_field)

    def test_field_mapping_should_return_adhoc_fields(self):
        mapping = field_mapping("ADHOC")

        self.assertEqual("fileName", mapping.file_name_field)
        self.assertEqual("catRef", mapping.catalogue_reference_field)

    def test_aws_config_should_return_pa_buckets(self):
        config = aws_config("test", "PA")

        self.assertEqual("test-pa-transfer", config.bucket_name)

    def test_aws_config_should_use_the_environment_for_pa_buckets(self):
        config = aws_config("prod", "PA")

        self.assertEqual("prod-pa-transfer", config.bucket_name)

    def test_aws_config_should_use_environment_for_adhoc_buckets(self):
        config = aws_config("test", "ADHOC")

        self.assertEqual("test-dr2-ingest-adhoc-cache", config.bucket_name)
