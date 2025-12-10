import argparse
import tempfile

from moto.utilities.utils import str2bool


def build():
    parser = argparse.ArgumentParser(
        description="Process an input CSV file to schedule corresponding ingests ",
        add_help=False
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="A CSV file containing details of the records to ingest. It must have columns (catRef, fileName, checksum)"
    )
    parser.add_argument(
        "-e", "--environment",
        help="Environment where the ingest is taking place (e.g. intg or prod)",
        default="intg"
    )
    parser.add_argument(
        "-d", "--dry_run",
        nargs="?",
        const=True,
        type=str2bool,
        help="Value of 'True' indicates that the tool will only validate inputs, without actually running an ingest",
        default=False
    )
    parser.add_argument(
        "-o", "--output",
        help="Name of the folder to store a CSV file representing generated metadata for this ingest",
        default=tempfile.gettempdir()
    )
    return parser
