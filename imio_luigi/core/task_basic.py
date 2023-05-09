# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget

import abc
import json
import luigi


class InMemoryTask(luigi.Task):
    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @abc.abstractmethod
    def transform_data(self, data):
        """Method that must be implemented"""
        return None

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)

    def output(self):
        return MockTarget(mock_filename(self, "InMemoryTask"), mirror_on_stderr=True)
