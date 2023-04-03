# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget

import abc
import copy
import json
import luigi


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
        for element in data[self.subelements_source_key]:
            new_element = self._create_subelement(element)
            data[self.subelements_destination_key].append(new_element)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class CreateSubElementsFromSubElementsInMemoryTask(
    CreateSubElementsFromSubElementsTask
):
    def output(self):
        return MockTarget(
            mock_filename(self, "CreateSubElementsFromSubElements"),
            mirror_on_stderr=True,
        )
