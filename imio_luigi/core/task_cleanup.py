# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget

import abc
import json
import luigi
import re


class StringToListTask(luigi.Task):
    """
    Convert a string to list based on specific separators
    """

    ignore_missing = True  # Define if an error must be thrown if a key is missing

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def attribute_key(self):
        """The string attribute key that need to be transformed into a list"""
        return None

    @property
    @abc.abstractmethod
    def separators(self):
        """The list of separators"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def _recursive_split(self, value, separators):
        regexp = f"({'|'.join(separators)})"
        return [v for v in re.split(regexp, value) if v and v not in separators]

    def transform_data(self, data):
        value = data.get(self.attribute_key, None)
        if isinstance(value, list):
            return data
        if value is None and self.ignore_missing is False:
            raise KeyError("Missing key {0}".format(self.attribute_key))
        elif value is None and self.ignore_missing is True:
            return data
        separators = [s for s in self.separators if s in value]
        if len(separators) > 0:
            value = self._recursive_split(value, separators)
        else:
            value = [value]
        data[self.attribute_key] = value
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class StringToListInMemoryTask(StringToListTask):
    def output(self):
        return MockTarget(mock_filename(self, "StringToList"), mirror_on_stderr=True)


class DropColumnTask(luigi.Task):
    """
    Drop columns
    """

    ignore_missing = True  # Define if an error must be thrown if a key is missing

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def drop_keys(self):
        """The list of keys to drop"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def transform_data(self, data):
        for key in self.drop_keys:
            if key not in data and self.ignore_missing is False:
                raise KeyError(f"Missing key 'key'")
            if key in data:
                del data[key]
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class DropColumnInMemoryTask(DropColumnTask):
    def output(self):
        return MockTarget(mock_filename(self, "DropColumn"), mirror_on_stderr=True)


class ValueFixerTask(luigi.Task):
    """
    Task that allow to fix incorrect values based on a set of rules
    """

    ignore_missing = True  # Define if an error must be thrown if a key is missing

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def rules_filepath(self):
        """
        File path to the rules files

        Excepted content format :
        {
            "key": [
                {
                    "regexp": "^regexp$",
                    "replacement": "new_value"
                }
            ]
        }
        """
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    @property
    def _rules(self):
        with open(self.rules_filepath, "r") as f:
            return json.load(f)

    def _apply_rule(self, value, rule):
        value = re.sub(rule["regexp"], rule["replacement"], value)
        return value

    def transform_data(self, data):
        for key, rules in self._rules.items():
            if key not in data and self.ignore_missing is False:
                raise KeyError(f"Missing key 'key'")
            if key in data:
                for rule in rules:
                    data[key] = self._apply_rule(data[key], rule)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class ValueFixerInMemoryTask(ValueFixerTask):
    def output(self):
        return MockTarget(mock_filename(self, "ValueFixer"), mirror_on_stderr=True)
