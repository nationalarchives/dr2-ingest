import argparse
import tempfile

from moto.utilities.utils import str2bool


def build():
    parser = argparse.ArgumentParser(
        description="Process an input CSV file to schedule the corresponding ingests",
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="A CSV file containing details of the records to ingest. It must contain the columns: catRef, fileName and checksum"
    )
    parser.add_argument(
        "-e", "--environment",
        help="The environment where the ingest is taking place (e.g. intg or prod)",
        default="intg"
    )
    parser.add_argument(
        "-d", "--dry-run",
        nargs="?",
        const=True,
        type=str2bool,
        help="A value of 'True' indicates that the tool will only validate the inputs without actually running an ingest",
        default=False
    )
    parser.add_argument(
        "-o", "--output",
        help="The name of the folder in which to store a CSV file representing generated metadata for this ingest",
        default=tempfile.gettempdir()
    )
    parser.add_argument(
        "-s", "--asset-source",
        choices=["Born Digital", "Surrogate", "Digitised"],
        help="The digital asset source. One of 'Born Digital', 'Surrogate' or 'Digitised'. The default is 'Born Digital'",
        default="Born Digital"
    )
    return parser
