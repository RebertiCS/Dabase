#!/usr/bin/env python3
#

import argparse
import sys

from src import api

version = "v1.0"


def Dabase():

    config = api.Config()

    if len(sys.argv) > 1:
        args = args_setup()
        config.args_parsed(args)
    else:
        config.args(
            src="./files/test/",
            file_type="csv",
            date_start="2025-10-10",
            output="Out/test.csv",
        )

    print(config.info())
    df_db = config.get_df()
    print(df_db)
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
        "-ds",
        "--date-start (%Y-%d-%m %H:%M:%s)",
        help="Start day of the date requested.",
    )

    parser.add_argument(
        "-de",
        "--date-end (%Y-%d-%m %H:%M:%s)",
        help="End day of the date requested.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    Dabase()
