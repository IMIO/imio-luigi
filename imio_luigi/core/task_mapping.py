# -*- coding: utf-8 -*-

from imio_luigi.core.utils import _cache, mock_filename
from luigi.mock import MockTarget

import abc
import json
import luigi


class MappingKeysTask(luigi.Task):
    """
    Convert keys with a specific mapping
    """

    ignore_missing = True  # Define if a missing key will be ignored

    @property
    @abc.abstractmethod
    def key(self):
        """The unique key"""
        return None

    @property
    @abc.abstractmethod
    def mapping(self):
        """The mapping dict"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def transform_data(self, data):
        """Transform data with the mapping table"""
        for original, destination in self.mapping.items():
            if self.ignore_missing is True:
                if original in data:
                    data[destination] = data[original]
                    del data[original]
            else:
                data[destination] = data[original]
                del data[original]
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class MappingKeysInMemoryTask(MappingKeysTask):
    def output(self):
        return MockTarget(mock_filename(self, "MappingKeys"), mirror_on_stderr=True)


class MappingValueTask(luigi.Task):
    """
    Convert value based on a mapping dictionary
    """

    # Define if an error must be thrown if a key is missing
    ignore_missing = True
    # Define if an error must be thrown if the mapping key is missing
    ignore_missing_mapping = True

    @property
    @abc.abstractmethod
    def mapping(self):
        """The dict that contains the mapping"""
        return None

    @property
    @abc.abstractmethod
    def mapping_key(self):
        """The key from the data that need to be mapped"""
        return None

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def custom_transform(self, data):
        """Method that can be overrided for custom transformation
        Should return transformed data if some transformation where made"""
        return None

    def transform_data(self, data):
        if self.mapping_key not in data and self.ignore_missing is False:
            raise KeyError(f"Missing key {self.mapping_key}")
        if self.mapping_key not in data:
            return data
        custom = self.custom_transform(data)
        if custom is not None:
            return custom
        new_value = self.mapping.get(data[self.mapping_key], None)
        if new_value is None and self.ignore_missing_mapping is False:
            raise KeyError(f"Missing key {data[self.mapping_key]}")
        elif new_value is None and self.ignore_missing_mapping is True:
            new_value = data[self.mapping_key]
        data[self.mapping_key] = new_value
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class MappingValueInMemoryTask(MappingValueTask):
    def output(self):
        return MockTarget(mock_filename(self, "MappingValue"), mirror_on_stderr=True)


class MappingValueWithFileTask(luigi.Task):
    """
    Convert value based on a mapping dictionary defined in a file
    """

    # Define if an error must be thrown if a key is missing
    ignore_missing = True
    # Define if an error must be thrown if the mapping key is missing
    ignore_missing_mapping = True

    @property
    @abc.abstractmethod
    def mapping_filepath(self):
        """The path to the file that contains the mapping"""
        return None

    @property
    @abc.abstractmethod
    def mapping_key(self):
        """The key from the data that need to be mapped"""
        return None

    @property
    @_cache(ignore_args=True)
    def mapping(self):
        mapping = json.load(open(self.mapping_filepath, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    @property
    @abc.abstractmethod
    def output(self):
        """The output target"""
        return None

    def custom_transform(self, data):
        """Method that can be overrided for custom transformation
        Should return transformed data if some transformation where made"""
        return None

    def transform_data(self, data):
        if self.mapping_key not in data and self.ignore_missing is False:
            raise KeyError(f"Missing key {self.mapping_key}")
        if self.mapping_key not in data:
            return data
        custom = self.custom_transform(data)
        if custom is not None:
            return custom
        new_value = self.mapping.get(data[self.mapping_key], None)
        if new_value is None and self.ignore_missing_mapping is False:
            raise KeyError(f"Missing key {data[self.mapping_key]}")
        elif new_value is None and self.ignore_missing_mapping is True:
            new_value = data[self.mapping_key]
        data[self.mapping_key] = new_value
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class MappingValueWithFileInMemoryTask(MappingValueWithFileTask):
    def output(self):
        return MockTarget(
            mock_filename(self, "MappingValueWithFile"), mirror_on_stderr=True
        )
