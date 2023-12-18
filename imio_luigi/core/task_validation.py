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

    log_failure = False  # Log in case of failure instead of raising an error

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

    def log_failure_output(self):
        fname = self.key.replace("/", "-")
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)

    def on_failure(self, data, errors):
        """Method that can be overrided on failure to do something specific
        This method is only called if `log_failure` is True

        data must be returned"""
        return data

    def _handle_exception(self, data, error, output_f):
        """Method called when an exception occured"""
        if not self.log_failure:
            raise error
        data = self.on_failure(data, [str(error)])
        json.dump(data, output_f)
        with self.log_failure_output().open("w") as f:
            error = {
                "error": str(error),
                "data": data,
            }
            f.write(json.dumps(error))

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
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try:
                    validate(data, self.json_schema)
                    json.dump(data, output_f)
                except Exception as e:
                    self._handle_exception(data, e.message, output_f)
