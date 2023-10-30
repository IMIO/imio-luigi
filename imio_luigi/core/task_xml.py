# -*- coding: utf-8 -*-

from datetime import datetime

import abc
import luigi
import pandas as pd
import numpy as np
import os

class GetFromXMLFile(luigi.Task):
    columns = ["*"]
    encoding = "utf-8"
    dtype = "string"

    @property
    @abc.abstractmethod
    def filepath(self):
        """The path to the table json dump"""
        return None

    def _col_filter(self, col):
        return self.columns == ["*"] or col in self.columns

    def query(self, min_range=None, max_range=None):
        """Return each row as a dict object"""
        pdata = pd.read_xml(
            self.filepath,
            encoding=self.encoding
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


class GetFromListXMLFile(GetFromXMLFile):
    files = []
    fail_on_error = True

    def transform_data(self, data, file):
        return data

    def query(self, min_range=None, max_range=None):
        for file in self.files:
            pdata = pd.read_xml(
                os.path.join(self.filepath, file),
                encoding=self.encoding
            )
            pdata = pdata.replace({np.nan: None})

            for nbr, line in pdata.iterrows():
                if min_range and nbr < min_range:
                    continue
                if max_range and nbr > max_range:
                    break
                data = line.to_dict()
                data["key"] = nbr
                try:
                    data = self.transform_data(data, file)
                except Exception as e:
                    if self.fail_on_error:
                        raise e
                    continue
                yield data
