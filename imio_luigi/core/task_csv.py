# -*- coding: utf-8 -*-

from datetime import datetime

import abc
import luigi
import pandas as pd
import numpy as np

class GetFromCSVFile(luigi.Task):
    columns = ["*"]
    delimiter= ","
    dtype={}

    @property
    @abc.abstractmethod
    def filepath(self):
        """The path to the table json dump"""
        return None

    def _col_filter(self, col):
        return self.columns == ["*"] or col in self.columns

    def query(self, min_range=None, max_range=None):
        """Return each row as a dict object"""
        pdata = pd.read_csv(
            self.filepath,
            delimiter=self.delimiter,
            usecols=self._col_filter,
            dtype=self.dtype
        )
        pdata = pdata.replace({np.nan: None})

        for nbr, line in pdata.iterrows():
            if min_range and nbr < min_range:
                continue
            if max_range and nbr > max_range:
                break
            data = line.to_dict()
            data["key"] = nbr
            yield data

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)