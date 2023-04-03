# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from jsonschema import validate, ValidationError
from luigi.mock import MockTarget

import abc
import json
import luigi


class JSONSchemaValidationTask(luigi.Task):
    """
    Validate JSON data with JSON Schema
    """

    @property
    @abc.abstractmethod
    def key(self):
        """The unique key"""
        return None

    @property
    @abc.abstractmethod
    def schema_path(self):
        """Path to JSON schema file"""
        return None

    def output(self):
        """The output target"""
        return MockTarget(
            mock_filename(self, "JSONSchemaValidation"), mirror_on_stderr=True
        )

    @property
    def json_schema(self):
        with open(self.schema_path, "r") as f:
            return json.load(f)

    def run(self):
        """There is nothing to do here"""
        try:
            with self.input().open("r") as input_f:
                with self.output().open("w") as output_f:
                    data = json.load(input_f)
                    validate(data, self.json_schema)
                    json.dump(data, output_f)
        except ValidationError as e:
            raise (e)
