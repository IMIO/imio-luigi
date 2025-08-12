# -*- coding: utf-8 -*-

from imio_luigi.core.utils import _cache, mock_filename
from luigi.mock import MockTarget

import abc
import json
import luigi

class RelationBaseTask(luigi.Task):
    """
    Link relationel files
    """

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            __import__("pdb").set_trace()
            data["description"]["data"] += f"<p>{error}</p>\r\n"
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

    @property
    def config_dict(self):
        """Dictionary with config"""
        return None

    @property
    def config_path(self):
        """Path to config file"""
        return None

    @property
    def config(self):
        config = {}
        if self.config_path is not None:
            with open(self.config_path, "r") as f:
                config = json.loads(f)
        if self.config_dict is not None:
            config = self.config_dict
        return config

    @abc.abstractmethod
    def get_data_from_file(self, file, pk=None):
        """
        Get data from file, must be override dpending the source

        :param file: path to the file
        :type file: string
        :param pk: Name of the key that will be used for primary key if None it will return a list, default None
        :type pk: string, optional
        :return: A dictionary or list of data
        :rtype: dict/list
        """
        return None

    def get_fk_in_data(self, data, fk_value, pk):
        if isinstance(data, dict):
            return data.get(fk_value, None)
        if isinstance(data, list):
            output = []
            for item in data:
                pk_value = item.get(pk, None)
                if pk_value is None:
                    continue
                if fk_value == pk_value:
                    output.append(item)
        return None

    def get_additonal_keys(self, config):
        output = []
        for key in config.keys():
            if key.startswith("_"):
                output.append(key)
        return output

    def handle_config(self, config, data, parent_data):
        file = config.get("file", None)
        fk = config.get("fk", None)
        pk = config.get("pk", None)
        relation = config.get("relation", None)
        bound = config.get("bound", None)
        if file is None or fk is None or pk is None or relation is None:
            raise KeyError("Missing one of important key (file, fk, pk or relation) in config")
        param = {"file": file}
        if relation.endswith("to_one"):
            param["pk"] = pk
        file_data = self.get_data_from_file(**param)
        parent_data_fk = parent_data.get(fk, None)
        if parent_data_fk is None:
            return None

        values = self.get_fk_in_data(file_data, parent_data_fk, pk)

        if not values:
            return None

        if bound:
            if isinstance(values, dict):
                bound_values = self.handle_config(add_config, data, values)
                if bound_values:
                    values = bound_values
            if isinstance(values, list):
                new_values = []
                for value in values:
                    bound_value = self.handle_config(add_config, data, value)
                    if not bound_value:
                        raise KeyError("Can't find key in file")
                    new_values.append(bound_value)
                values = new_values

        for add_key in self.get_additonal_keys(config):
            add_config = config[add_key]
            add_key = add_key.lstrip("_")
            if isinstance(values, dict):
                add_values = self.handle_config(add_config, data, values)
                if not add_values:
                    continue
                values[add_key] = add_values
            if isinstance(values, list):
                new_values = []
                for value in values:
                    add_value = self.handle_config(add_config, data, value)
                    if add_value:
                        value[add_key] = add_value
                    new_values.append(value)
                values = new_values
        return values

    def transform_data(self, data):
        """Transform data"""
        for key, config in self.config.items():
            value = self.handle_config(config, data=data, parent_data=data)
            if not value:
                continue
            data[key] = value
        return data

    def run(self):
            with self.input().open("r") as input_f:
                with self.output().open("w") as output_f:
                    data = json.load(input_f)
                    try:
                        json.dump(self.transform_data(data), output_f)
                    except Exception as e:
                        self._handle_exception(data, e, output_f)


class RelationAcessTask(RelationBaseTask):
    def get_data_from_file(self, file, pk=None):
        if pk:
            output = {}
        else:
            output = []
        with open(file, "r") as f:
            for line in f:
                data = json.loads(line)
                if pk:
                    if pk not in data:
                        raise KeyError(f"PK ({pk}) not found in data")
                    if data[pk] in output:
                        raise KeyError(f"PK ({pk}) already in output values")
                    output[data[pk]] = data
                else:
                    output.append(data)
        return output


class RelationAcessInMemoryTask(RelationAcessTask):
    def output(self):
        return MockTarget(mock_filename(self, "RelationAcess"), mirror_on_stderr=True)
