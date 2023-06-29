# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget

import abc
import copy
import json
import luigi
import re


class AddDataTask(luigi.Task):
    """
    Add data from a static file
    """

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def filepath(self):
        """The filepath to the source data file"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def transform_data(self, data):
        with open(self.filepath, "r") as source:
            new_data = json.load(source)
            for key, value in new_data.items():
                data[key] = value
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class AddDataInMemoryTask(AddDataTask):
    def output(self):
        return MockTarget(mock_filename(self, "AddData"), mirror_on_stderr=True)


class CreateSubElementTask(luigi.Task):
    """
    Transform some attributes into a sub element
    """

    create_container = True  # Do not raise an error if the container key does not exist
    ignore_missing = True  # Define if an error must be thrown if a key is missing

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def subelement_container_key(self):
        """The key of the subelement container"""
        return None

    @property
    @abc.abstractmethod
    def mapping_keys(self):
        """The mapping keys that will be moved to the sub element"""
        return None

    @property
    @abc.abstractmethod
    def subelement_base(self):
        """The dict with the base element (including values) for the subelement"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def transform_data(self, data):
        if self.subelement_container_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelement_container_key}")
            data[self.subelement_container_key] = []
        new_element = copy.deepcopy(self.subelement_base)
        for key, destination in self.mapping_keys.items():
            if key not in data and self.ignore_missing is False:
                raise KeyError(f"Missing key {key}")
            if key in data:
                new_element[destination] = data[key]
                del data[key]
        data[self.subelement_container_key].append(new_element)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class CreateSubElementInMemoryTask(CreateSubElementTask):
    def output(self):
        return MockTarget(
            mock_filename(self, "CreateSubElement"), mirror_on_stderr=True
        )


class CreateSubElementsFromSubElementsTask(luigi.Task):
    """
    Transform subelements into other subelements using a mapping dict
    """

    create_container = True  # Do not raise an error if the container key does not exist
    ignore_missing = True  # Define if an error must be thrown if a key is missing
    log_failure = False  # Log in case of failure instead of raising an error

    @property
    @abc.abstractmethod
    def key(self):
        """The unique id of this record"""
        return None

    @property
    @abc.abstractmethod
    def subelements_source_key(self):
        """The key of the subelement container"""
        return None

    @property
    @abc.abstractmethod
    def subelements_destination_key(self):
        """The key of the subelement container"""
        return None

    @property
    @abc.abstractmethod
    def mapping_keys(self):
        """The mappin keys that will be moved to the sub element"""
        return None

    @property
    @abc.abstractmethod
    def subelement_base(self):
        """The dict with the base element (including values) for the subelement"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

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

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)

    def _create_subelement(self, base):
        new_element = copy.deepcopy(self.subelement_base)
        for key, destination in self.mapping_keys.items():
            if key not in base and self.ignore_missing is False:
                raise KeyError(f"Missing key {key}")
            if key in base:
                new_element[destination] = base[key]
                del base[key]
        return new_element

    def transform_data(self, data):
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
        if self.subelements_source_key not in data:
            if self.ignore_missing is False:
                raise KeyError(f"Missing key {self.subelements_source_key}")
            return data
        for element in data[self.subelements_source_key]:
            new_element = self._create_subelement(element)
            data[self.subelements_destination_key].append(new_element)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try:
                    data = self.transform_data(data)
                    json.dump(data, output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)


class CreateSubElementsFromSubElementsInMemoryTask(
    CreateSubElementsFromSubElementsTask
):
    def output(self):
        return MockTarget(
            mock_filename(self, "CreateSubElementsFromSubElements"),
            mirror_on_stderr=True,
        )


class UpdateReferenceTask(luigi.Task):
    """Task that allow to update reference to match a Regexp"""

    log_failure = False  # Log in case of failure instead of raising an error

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
            "reference_key": [
                {
                    "conditions": {
                        "key1": "condition",
                        "key2": "condition"
                    },
                    "regexp": "^regexp$",
                    "replacement": "new_value",
                    "expected_regexp": "^regexp$"
                }
            ]
        }

        conditions : A list a matching condition that is required
                    to apply this regexp
        regexp : The regexp that need to match the element that should be
                 replaced
        replacement : The new value
        expected_regexp : The regexp that validate the new value result
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

    def on_failure(self, data, errors):
        """Method that can be overrided on failure to do something specific
        This method is only called if `log_failure` is True

        data must be returned"""
        return data

    def log_failure_output(self):
        fname = self.key.replace("/", "-")
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)

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

    def _handle_failure(self, data, errors, output_f):
        """Method called when errors occured but they are handled"""
        if not self.log_failure:
            raise ValueError(", ".join(errors))
        data = self.on_failure(data, errors)
        json.dump(data, output_f)
        with self.log_failure_output().open("w") as f:
            error = {
                "error": ", ".join(errors),
                "data": data,
            }
            f.write(json.dumps(error))

    def _apply_rule(self, data, key, rule):
        value = data[key]
        # Verify condition
        for c_key, condition in rule["conditions"].items():
            if data[c_key] != condition:
                return value
        # Verify result before
        if re.match(rule["expected_regexp"], value):
            return value
        value = re.sub(rule["regexp"], rule["replacement"], value)
        if not re.match(rule["expected_regexp"], value):
            raise ValueError(
                f"Value '{value}' does not match regexp '{rule['expected_regexp']}'"
            )
        return value

    def transform_data(self, data):
        errors = []
        for key, rules in self._rules.items():
            if key not in data and self.ignore_missing is False:
                errors.append(f"Missing key '{key}'")
                continue
            if key in data:
                for rule in rules:
                    data[key] = self._apply_rule(data, key, rule)
        return data, errors

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try:
                    data, errors = self.transform_data(data)
                    if len(errors) > 0:
                        self._handle_failure(data, errors, output_f)
                    else:
                        json.dump(data, output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)


class UpdateReferenceInMemoryTask(UpdateReferenceTask):
    def output(self):
        return MockTarget(mock_filename(self, "UpdateReference"), mirror_on_stderr=True)
