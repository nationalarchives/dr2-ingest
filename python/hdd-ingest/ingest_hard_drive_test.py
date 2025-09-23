import argparse
from unittest import TestCase

import ingest_hard_drive


class Test(TestCase):
    def setUp(self):
        self.parser = ingest_hard_drive.build_argument_parser()

    def test_should_fail_when_mandatory_argument_is_missing(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args([])

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["-i"])

    def test_should_parse_mandatory_arguments_and_return_default_for_optional_arguments(self):
        args = self.parser.parse_args(["-i", "some_file.csv"])
        self.assertEqual("some_file.csv", args.input)
        self.assertEqual(False, args.dry_run)
        self.assertEqual("INTG", args.environment)

    def test_should_parse_arguments_and_set_correct_parameters_for_arguments_passed_on_command_line(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d", "True"])
        self.assertEqual("some_file.csv", args.input)
        self.assertTrue(args.dry_run)
        self.assertEqual("not_prod", args.environment)

    def test_should_error_when_the_input_file_does_not_exist(self):
        args = argparse.Namespace(input='non_existent_file.csv', environment='not_prod', dry_run='True')
        with self.assertRaises(Exception) as e:
            ingest_hard_drive.validate_arguments(args)

        self.assertEqual("The input file [non_existent_file.csv] does not exist or it is not a valid file\n", str(e.exception))



