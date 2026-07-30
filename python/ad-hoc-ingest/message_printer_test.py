import io
import unittest
from contextlib import redirect_stdout, redirect_stderr

import message_printer as mp


def get_current_console_display(entire_console_operation):
    current_line = ""
    for character in entire_console_operation:
        if character == "\r":
            current_line = ""
        else:
            current_line += character

    return current_line


class MyTestCase(unittest.TestCase):
    def test_print_message_should_print_a_progress_message_to_the_console(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            mp.print_message("some test message")

        output = stdout.getvalue()
        self.assertIn("some test message", output)

    def test_progress_message_should_print_a_progress_message_to_the_console(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            mp.print_progress("some test message")

        output = stdout.getvalue()
        self.assertIn("some test message", output)

    def test_second_progress_message_should_wipe_out_the_current_line_and_print_new_message(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            mp.print_progress("Progress: 2 of 6 complete")
            mp.print_progress("Progress: 4 of 6 complete")

        output = stdout.getvalue()
        current_line = get_current_console_display(output)

        self.assertIn("Progress: 4 of 6 complete", current_line)
        self.assertNotIn(current_line, "Progress: 2 of 6 complete")

    def test_print_message_should_wipe_previous_progress_message_and_write_new_message(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            mp.print_progress("Progress: 2 of 6 complete")
            mp.print_message("Progress: 4 of 6 complete")

        output = stdout.getvalue()
        current_line = get_current_console_display(output)

        self.assertIn("Progress: 4 of 6 complete", current_line)
        self.assertNotIn(current_line, "Progress: 2 of 6 complete")

    def test_print_message_should_preserve_previous_print_message_and_write_new_message(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            mp.print_progress("Progress: 2 of 6 complete")
            mp.print_message("Progress: 6 of 6 complete")
            mp.print_message("Process completed successfully")

        output = stdout.getvalue()
        current_line = get_current_console_display(output)

        self.assertIn("Progress: 6 of 6 complete", current_line)
        self.assertIn("Process completed successfully", current_line)
        self.assertNotIn("Progress: 2 of 6 complete", current_line)

    def test_print_error_should_print_message_from_exception_with_additional_error_header(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            mp.print_error(Exception("Expected something useful but got nothing"))

        output = stderr.getvalue()
        self.assertIn("*** ERROR ***", output)
        self.assertIn("Expected something useful but got nothing", output)
