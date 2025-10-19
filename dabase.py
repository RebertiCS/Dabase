#!/usr/bin/env python3
#

import argparse
import sys

from src import api

version = "v1.0"


def Dabase(config=None):
    """Main function"""
    if config is None:
        config = api.Config()

        if len(sys.argv) > 1:
            args = args_setup()
            config.args_parsed(args)

    config.get_df()
    config.create()


def args_setup():
    """Setup arguments for cli"""
    parser = argparse.ArgumentParser(
        prog="dabase.py",
        description="Database aggregator",
        epilog="Database aggregator - v1.0 - GPLv2 rebertics+dev@gmail.com @ 2025.",
    )

    parser.add_argument(
        "-s",
        "--src",
        help="Source directory",
    )

    parser.add_argument(
        "-c",
        "--create",
        help="Create file",
        action="store_true",
    )

    parser.add_argument(
        "-f",
        "--file",
        help="Type of output (xlsx, csv, parquet, postgres)",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Output path for command.",
    )

    parser.add_argument(
        "--date-col",
        help="Date column in dataframe",
    )

    parser.add_argument(
        "-ds",
        "--date-start",
        help="Start day of the date requested. format: (%Y-%d-%m %H:%M:%s)",
    )

    parser.add_argument(
        "-de",
        "--date-end",
        help="End day of the date requested. format: (%Y-%d-%m %H:%M:%s)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    Dabase(None)
