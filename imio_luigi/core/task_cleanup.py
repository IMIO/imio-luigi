# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi.core.utils import mock_filename
from imio_luigi.utils.helpers import get_value_from_path
from imio_luigi.utils.helpers import set_value_from_path
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

    def _fix_item(self, item):
        """Called on every item after splitting to fix or transform each item"""
        return item

    def transform_data(self, data):
        value = data.get(self.attribute_key, None)
        if isinstance(value, list):
            return data
        if value is None and self.ignore_missing is False:
            raise KeyError("Missing key {0}".format(self.attribute_key))
        elif value is None and self.ignore_missing is True:
            return data
        separators = [s for s in self.separators if re.search(s, value)]
        if len(separators) > 0:
            value = self._recursive_split(value, separators)
        else:
            value = [value]
        data[self.attribute_key] = [self._fix_item(item) for item in value]
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class StringToListInMemoryTask(StringToListTask):
    def output(self):
        return MockTarget(mock_filename(self, "StringToList"), mirror_on_stderr=True)


class StringToListRegexpTask(StringToListTask):
    def _recursive_split(self, value, separators):
        pattern = rf"\s*(?:{'|'.join(separators)})\s*"
        return re.split(pattern, value)

    def transform_data(self, data):
        value = data.get(self.attribute_key, None)
        if isinstance(value, list):
            return data
        if value is None and self.ignore_missing is False:
            raise KeyError("Missing key {0}".format(self.attribute_key))
        elif value is None and self.ignore_missing is True:
            return data

        separators = self.separators
        if separators and len(separators) > 0:
            value = self._recursive_split(value, separators)
        else:
            value = [value]
        data[self.attribute_key] = [self._fix_item(item) for item in value]
        return data


class StringToListRegexpInMemoryTask(StringToListRegexpTask):
    def output(self):
        return MockTarget(mock_filename(self, "StringToListRegexp"), mirror_on_stderr=True)


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
                raise KeyError(f"Missing key '{key}'")
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
            "key_to_fix": [
                {
                    "regexp": "^regexp$",
                    "replacement": "new_value"
                }
            ]
        }
        key_to_fix : key in data that will be fix
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
                raise KeyError(f"Missing key '{key}'")
            if key in data:
                for rule in rules:
                    data[key] = self._apply_rule(data[key], rule)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                try:
                    data = json.load(input_f)
                except json.decoder.JSONDecodeError as err:
                    __import__('pdb').set_trace()
                    pass
                json.dump(self.transform_data(data), output_f)


class ValueFixerInMemoryTask(ValueFixerTask):
    def output(self):
        return MockTarget(mock_filename(self, "ValueFixer"), mirror_on_stderr=True)


class ConvertDateTask(luigi.Task):
    """Task that allow to converts date in string into a datetime object"""

    ignore_missing = True  # Define if an error must be thrown if a key is missing
    log_failure = False  # Log in case of failure instead of raising an error

    @abc.abstractproperty
    def key(self):
        """The unique id of this record"""
        return None

    @abc.abstractproperty
    def keys(self):
        """The list of keys that contains date"""
        return None

    @abc.abstractproperty
    def date_format_input(self):
        """The format (in string) of the dates"""
        return None

    @abc.abstractproperty
    def date_format_output(self):
        """The format (in string) of the dates"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    @property
    def key_to_ignore_exception(self):
        return ()

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

    def _handle_exception(self, data, error):
        """Method called when an exception occured"""
        if not self.log_failure:
            raise error
        data = self.on_failure(data, [str(error)])
        with self.log_failure_output().open("w") as f:
            error = {
                "error": str(error),
                "data": data,
            }
            f.write(json.dumps(error))

    def format_date(self, input_date):
        date = datetime.strptime(input_date, self.date_format_input)
        return date.strftime(self.date_format_output)

    def transform_data(self, data):
        for key in self.keys:
            if not get_value_from_path(data, key) and self.ignore_missing is False:
                raise KeyError(f"Missing key '{key}'")
            if get_value_from_path(data, key):
                try:
                    set_value_from_path(
                        data,
                        key,
                        self.format_date(get_value_from_path(data, key))
                    )
                    # data[key] = self.format_date(data[key])
                except ValueError as e:
                    if key not in self.key_to_ignore_exception:
                        self._handle_exception(data, e)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class ConvertDateInMemoryTask(ConvertDateTask):
    def output(self):
        return MockTarget(mock_filename(self, "ConvertDate"), mirror_on_stderr=True)


class ConvertDateTaskMultiFormat(ConvertDateTask):
    @abc.abstractproperty
    def date_format_input(self):
        """List of possible format (in string) of the dates"""
        return None

    def format_date(self, input_date):
        error = None
        result = None
        for input_format in self.date_format_input:
            try:
                date = datetime.strptime(input_date, input_format)
                result = date.strftime(self.date_format_output)
            except ValueError as e:
                error = e
                continue
        if result is None and error is not None:
            raise ValueError(error)
        return result


class ConvertDateInMultiFormatMemoryTask(ConvertDateTaskMultiFormat):
    def output(self):
        return MockTarget(mock_filename(self, "ConvertDateMultiFormat"), mirror_on_stderr=True)
