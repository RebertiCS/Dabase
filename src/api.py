#!/usr/bin/env python3
import sys
import os

from datetime import datetime

import pandas as pd

import dabase


class Config:

    src = None
    file_type = None
    date_col = None
    date_start = None
    date_end = None
    output = None

    df = None

    def args(
        self,
        src,
        date_col=None,
        file_type=None,
        date_start=None,
        date_end=None,
        output=None,
    ):
        self.src = src
        self.date_col = date_col
        self.file_type = file_type
        self.date_start = pd.to_datetime(date_start)
        self.date_end = pd.to_datetime(date_end)
        self.output = output

    def args_parsed(self, args):

        self.src = args.src
        self.date_col = args.date_col
        self.file_type = args.file
        self.date_start = pd.to_datetime(args.date_start)
        self.date_end = pd.to_datetime(args.date_end)
        self.output = args.output

    def info(self):
        """Information about vars stored"""
        print(self.src)
        print(self.file_type)
        print(self.date_start)
        print(self.date_end)
        print(self.output)

    def get_df(self):
        self.df = get_db(self.src)

        if self.date_col is not None:

            if self.date_start:
                date = self.date_start.date()
                self.df = self.df.query(f"{self.date_col} >= @date")

            if self.date_end:
                date = self.date_end.date()
                self.df = self.df.query(f"{self.date_col} <= @date")

    def create(self):
        if self.file_type == "csv":
            self.df.to_csv(self.output)

        elif self.file_type == "xlsx":
            self.df.to_excel(self.output)

        elif self.file_type == "parquet":
            self.df.to_parquet(self.output, compression="brotli")


def get_db(src):
    """Get df from files in src."""
    try:
        csv_list = os.listdir(src)

    except OSError:
        print(
            f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] System failed to read the files.",
        )
        sys.exit(2)

    except TypeError:
        print(
            f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Invalid folder path.",
        )
        sys.exit(2)

    except PermissionError:
        print(
            f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Missing permission.",
        )
        sys.exit(2)

    if len(csv_list) == 0:
        print(
            f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] No files found",
        )
        sys.exit(2)

    new_df = pd.DataFrame()

    for file_name in csv_list:

        df = None

        try:
            file_path = f"{src}/{file_name}"
            extension = str(file_name).split(".")

            if extension[-1] == "parquet":
                df = pd.read_parquet(file_path, engine="pyarrow")
            elif extension[-1] == "xlsx":
                df = pd.read_excel(file_path)
            elif extension[-1] == "csv":
                df = pd.read_csv(file_path, sep=";")

        except Exception as e:
            print(
                f"[ LogSql {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Erro: {e}"
            )
            continue

        print(
            f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Processing: {file_name}"
        )

        if isinstance(new_df, int):
            print(
                f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Failed processing: {file_name}"
            )
        else:
            print(
                f"[ Dabase {dabase.version} - {datetime.now().strftime('%H:%M:%S')} ] Finalized file: {file_name}"
            )

            new_df = pd.concat([df, new_df], axis=0)

    return new_df
