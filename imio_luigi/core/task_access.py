# -*- coding: utf-8 -*-

import abc
import json
import luigi


class GetFromAccessJSONTask(luigi.Task):
    """Task to get informations from an exported Microsoft Access table in JSON"""

    encoding = "utf-8"
    columns = ["*"]

    @property
    @abc.abstractmethod
    def filepath(self):
        """The path to the table json dump"""
        return None

    def query(self, min_range=None, max_range=None):
        """Return each row as a dict object"""
        with open(self.filepath, "r") as f:
            for nbr, line in enumerate(f):
                if min_range and nbr < min_range:
                    continue
                if max_range and nbr > max_range:
                    break
                data = json.loads(line)
                if self.columns != ["*"]:
                    data = {k: v for k, v in data.items() if k in self.columns}
                yield data

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)
