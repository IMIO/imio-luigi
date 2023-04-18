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

    def query(self):
        """Return each row as a dict object"""
        with open(self.filepath, "r") as f:
            for line in f.readlines():
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
