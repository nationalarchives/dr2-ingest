import shutil
import tempfile
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from samba.dcerpc.lsa import String

import argument_parser_builder

class Test(TestCase):
    def setUp(self):
        self.parser = argument_parser_builder.build()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("sys.stderr", new_callable=StringIO)
    def test_parse_args_should_fail_when_mandatory_input_argument_is_missing(self, mock_err):
        with self.assertRaises(SystemExit):
            self.parser.parse_args([])
        self.assertIn("error: the following arguments are required: -i/--input", mock_err.getvalue().strip().splitlines()[-1])

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["-i"])

        self.assertIn("error: argument -i/--input: expected one argument", mock_err.getvalue().strip().splitlines()[-1])

    def test_should_parse_mandatory_arguments_and_return_default_for_optional_arguments(self):
        args = self.parser.parse_args(["-i", "some_file.csv"])
        self.assertEqual("some_file.csv", args.input)
        self.assertEqual(False, args.dry_run)
        self.assertEqual("intg", args.environment)

    def test_should_treat_dry_run_param_as_true_when_no_option_is_provided(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d"])
        self.assertEqual(True, args.dry_run)

        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "--dry_run"])
        self.assertEqual(True, args.dry_run)

    def test_should_set_the_output_folder_as_temp_location_if_not_passed_as_a_parameter(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d"])
        self.assertEqual(tempfile.gettempdir(), args.output)


    def test_should_parse_arguments_and_set_correct_parameters_for_arguments_passed_on_command_line(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d", "True", "-o", "/home/Users"])
        self.assertEqual("some_file.csv", args.input)
        self.assertTrue(args.dry_run)
        self.assertEqual("not_prod", args.environment)
        self.assertEqual("/home/Users", args.output)
