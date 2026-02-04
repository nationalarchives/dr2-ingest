import os
import shutil
import tempfile
from io import StringIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import metadata_creator
from discovery_client import CollectionInfo, RecordDetails

class Test(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)


    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_create_a_metadata_object_with_value_for_each_field_in_fieldnames(self, mock_former_references, mock_description):
        mock_former_references.return_value = RecordDetails("Dept Ref", "TNA Ref")
        mock_description.return_value = CollectionInfo("some_id", None, "Some description from discovery")

        field_names = metadata_creator.get_field_names()

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8 / 3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual(len(field_names), len(metadata))
            self.assertTrue(all(f in metadata for f in field_names))

    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_create_a_metadata_object_from_csv_rows(self, mock_former_references, mock_description):
        mock_former_references.return_value = RecordDetails("Dept Ref", "TNA Ref")
        mock_description.return_value = CollectionInfo("some_id", None, "Some description from discovery")

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8 / 3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("JS 8", metadata["Series"])
            self.assertEqual("evid0001.pdf", metadata["Filename"])
            self.assertEqual("3", metadata["FileReference"])
            self.assertEqual("9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc", metadata["checksum_sha256"])
            self.assertEqual("Some description from discovery", metadata["description"])
            self.assertEqual("d:\\js\\3\\1\\evid0001.pdf", metadata["ClientSideOriginalFilepath"])
            self.assertEqual("TNA Ref", metadata["formerRefTNA"])
            self.assertEqual("Dept Ref", metadata["formerRefDept"])
            self.assertEqual("some_id", metadata["IAID"])

    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_create_a_metadata_object_with_title_from_discovery_containing_comma(self,
                                                                                                     mock_former_references,
                                                                                                     mock_collection_info):
        mock_former_references.return_value = RecordDetails("Dept Ref", "TNA Ref")
        mock_collection_info.return_value = CollectionInfo("some_id", None, "Some information about Kew, Richmond, London")

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("JS 8", metadata["Series"])
            self.assertEqual("evid0001.pdf", metadata["Filename"])
            self.assertEqual("3", metadata["FileReference"])
            self.assertEqual("9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc", metadata["checksum_sha256"])
            self.assertEqual("Some information about Kew, Richmond, London", metadata["description"])
            self.assertEqual("d:\\js\\3\\1\\evid0001.pdf", metadata["ClientSideOriginalFilepath"])
            self.assertEqual("TNA Ref", metadata["formerRefTNA"])
            self.assertEqual("Dept Ref", metadata["formerRefDept"])
            self.assertEqual("some_id", metadata["IAID"])

    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_create_a_filename_from_various_paths_independent_of_platform(self, mock_former_references, mock_collection_info):
        mock_former_references.return_value = RecordDetails("dept_ref", "tna_ref")
        mock_collection_info.return_value = CollectionInfo("some_id", None, "Some description from discovery")

        csv_data = """catRef,fileName,checksum
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,windows_absolute_path
        JS 8/4,c:\\evid0001.pdf,windows_absolute_path_at_root
        JS 8/5,c:\old_folder\evid0001.pdf,windows_absolute_path_single_slash
        JS 8/6,/home/users/evid0001.pdf,unix_absolute_path
        JS 8/7,c:evid0001.pdf,windows_no_slashes
        JS 8/7,\\a\\b\\evid0001.pdf,windows_relative_single_slash
        JS 8/8,c:/abcd/evid0001.pdf,windows_absolute_path_forward_slash"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("evid0001.pdf", metadata["Filename"])

    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_use_title_as_description_when_title_is_available_from_discovery(self, mock_former_references, mock_collection_info):
        mock_former_references.return_value = RecordDetails("A", "B")
        mock_collection_info.return_value = CollectionInfo("some_id", "Some title", "Some description from discovery")

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("Some title", metadata["description"])


    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_not_create_dept_ref_when_it_does_not_exist(self, mock_former_references, mock_collection_info):
        mock_former_references.return_value = RecordDetails(None, "TNA Ref")
        mock_collection_info.return_value = CollectionInfo("some_id", "Some title", "Some description from discovery")

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("TNA Ref", metadata["formerRefTNA"])
            self.assertEqual("", metadata["formerRefDept"])


    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_create_an_md5_hash_if_checksum_is_missing_from_the_input(self, mock_former_references, mock_collection_info):
        mock_former_references.return_value = RecordDetails(None, "TNA Ref")
        mock_collection_info.return_value = CollectionInfo("some_id", "Some title", "Some description from discovery")

        tmp1 = os.path.join(self.test_dir, "ad_hoc_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        csv_data = f"""catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,duplicate_value_allowed_here,{tmp1},,another"""
        data_set = pd.read_csv(StringIO(csv_data), dtype={"checksum": str}, keep_default_na=False)
        for index, row in data_set.iterrows():
            metadata = metadata_creator.create_intermediate_metadata_dict(row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))
            self.assertEqual("3a16291a00172e7af139cef48d1fe2f7", metadata["checksum_md5"])

    @patch("discovery_client.get_title_and_description")
    @patch("discovery_client.get_former_references")
    def test_create_metadata_should_throw_exception_when_it_cannot_find_title_or_description_from_discovery(self, mock_former_ref, mock_description):
        mock_former_ref.return_value = RecordDetails("A", "B")
        mock_description.return_value = CollectionInfo("some_id", "", "")

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            someTestCatRef,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        first_row = data_set.iloc[0]

        with self.assertRaises(Exception) as e:
            metadata_creator.create_intermediate_metadata_dict(first_row, SimpleNamespace(environment="test", input="/home/users/input-file.csv"))

        self.assertEqual("Title and Description both are empty for 'someTestCatRef', unable to proceed with this record", str(e.exception))

