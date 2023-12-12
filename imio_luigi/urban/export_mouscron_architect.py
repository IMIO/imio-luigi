# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.core.utils import _cache
from imio_luigi.urban import tools
from datetime import datetime
from luigi.parameter import ParameterVisibility

import json
import logging
import luigi
import os
import re
import copy
import numpy as np
import pandas as pd
import requests

logger = logging.getLogger("luigi-interface")

class GetFromCSV(core.GetFromCSVFile):
    task_namespace = "mouscron-architect"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    delimiter = ";"
    dtype = "string"
    url = luigi.Parameter()
    login = luigi.OptionalParameter(default="")
    password = luigi.OptionalParameter(
        default="", visibility=ParameterVisibility.PRIVATE
    )

    def _complete(self, key, data):
        """Method to speed up process that verify if output exist or not"""
        task = WriteToJSON(key=key)
        return task.complete()

    def _fix_character(self, term, character):
        if character in term:
            joiner = '"{}"'.format(character)
            term = joiner.join(term.split(character))
        return term

    def _fix_key(self, key):
        key = self._fix_character(key, "(")
        key = self._fix_character(key, ")")
        return key

    def search_architect(self, key, obj_type, search_on = "SearchableText"):
        mapping_type = {
            "ARCHITECTE": "architects",
            "GEOMETRE": "geometricians"
        }
        auth = None
        if self.login and self.password:
            auth = (self.login, self.password)
        params = {search_on: f"{self._fix_key(key)}"}
        search_url = f"{self.url}/urban/{mapping_type[obj_type]}/@search"
        r = requests.get(
            search_url,
            headers={"Accept": "application/json"},
            auth=auth,
            params=params,
        )
        if not hasattr(r, "status_code"):
            raise RuntimeError("Request result has no status code")
        if r.status_code != 200:
            message = (
                "Wrong request returned status_code {0}, expected codes is: 200, "
                "message: {1}"
            )
            raise RuntimeError(
                message.format(
                    r.status_code,
                    r.content,
                )
            )
        return r.json()["items_total"] >= 1

    def _generate_plone_id(self, data):
        data["old_id"] = data["id"]
        id = data["nom"]
        name2 = data.get("prenom", None)
        if name2 and name2 != '.' :
            id = f"{id}-{name2}"
        id = id.lower()
        id = id.replace(" ", "_")
        id = id.replace("\n", "_")
        id = id.replace("\r", "_")
        id = id.replace("/", "_")
        id = id.replace(".", "-")
        id = id.replace("é", "e")
        id = id.replace("ê", "e")
        id = id.replace("è", "e")
        id = id.replace("ë", "e")
        id = id.replace("à", "a")
        id = id.replace("â", "a")
        id = id.replace("ô", "o")
        id = id.replace("ö", "o")
        id = id.replace("û", "u")
        id = id.replace("ü", "u")
        id = id.replace("ç", "c")
        id = id.replace("&", "et")
        id = id.replace("&", "et")
        id = id.replace("²", "2")
        id = id.replace("(", "")
        id = id.replace(")", "")
        id = id.replace("'", "-")
        id = id.replace("_-_", "-")
        id = id.replace("-_", "-")
        id = id.replace("_-", "-")
        id = id.replace("__", "_")
        id = id.replace("___", "_")
        id = re.sub(r"-$","", id)
        data["id"] = id
        return data

    def run(self):
        min_range = None
        max_range = None
        counter = None
        if self.line_range:
            if not re.match(r"\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            min_range = int(line_range[0])
            max_range = int(line_range[1])
        if self.counter:
            counter = int(self.counter)
        iteration = 0
        for row in self.query(min_range=min_range, max_range=max_range):
            row = self._generate_plone_id(row)
            if row["type_list"] not in ["ARCHITECTE", "GEOMETRE"]:
                continue
            
            failure_output = luigi.LocalTarget(f"./failures/{self.task_namespace}-existing_contact/{row['old_id']}.json")
            if self._complete(row["id"], row) is True:
                with failure_output.open("w") as f:
                    row["error"] = "existe en local"
                    json.dump(row, f)
                continue
            if self.search_architect(row["id"],row["type_list"], "id") is True :
                with failure_output.open("w") as f:
                    row["error"] = "existe en prod avec le même id"
                    json.dump(row, f)
                continue
            if self.search_architect(row["nom"],row["type_list"]) is True :
                with failure_output.open("w") as f:
                    row["error"] = "existe en prod avec le même name"
                    json.dump(row, f)
                continue

            try:
                yield Transform(key=row["id"], data=row)
            except Exception as e:
                with self.log_failure_output().open("w") as f:
                    error = {
                        "error": str(e),
                        "data": row,
                    }
                    f.write(json.dumps(error))
            iteration += 1
            if counter and iteration >= counter:
                break
            

class Transform(luigi.Task):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    log_failure = False
    whitelist = [
        "utilisateur_fk",
        "type_permis_fk",
        "organisme_fk",
        "localite_fk",
        "division_fk",
        "parcelle_info_fk",
        "civilite_fk",
        "pays_fk",
        "rue_fk"
    ]

    @property
    def get_fk_table_mapping(self):
        with open("./config/mouscron/fk_table_mapping-mouscron.json", "r") as f:
            data = json.load(f)
        return data

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            __import__('pdb').set_trace()
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

    def get_table(self, table, id, key, orient="dict"):
        folder = table[0]
        pdata = pd.read_csv(
            f"./data/mouscron/{folder}/{table}.csv",
            delimiter=";",
            dtype="string"
        )
        pdata = pdata.replace({np.nan: None})
        result = pdata.loc[pdata[key] == id]
        if orient == 'dict' and len(result) == 1:
            return result.squeeze().to_dict()
        return result.to_dict(orient=orient)

    def add_outside_data(self, data, table, key):
        values = self.get_table(table, self.key, key, orient="records")
        values = [self.populate_cross_data(value, whitelist = self.whitelist) for value in values]
        data[table] = values
        return data

    def populate_cross_data(self, data, blacklist = [], whitelist = []):
        if type(data) == str:
            return data
        for key in data:
            if type(data[key]) is dict or key == "permis_fk" or key in blacklist:
                continue
            if key in self.get_fk_table_mapping and data[key]:
                table = self.get_fk_table_mapping[key]["table"]
                key_table = self.get_fk_table_mapping[key]["key"]
                value = self.get_table(table, data[key], key_table)
                data[key] = self.populate_cross_data(value, whitelist = self.whitelist)
        return data

    def _get_value_from_path(self, data, path):
        path_split = path.split("/")
        current_data = data
        for key in path_split:
            if isinstance(current_data, dict) and key in current_data:
                current_data = current_data[key]
            else:
                return None
        return current_data
        
    def extract_data(self, data, from_path, to):
        result = utils.get_value_from_path(data, from_path)
        if not result:
            return data
        data[to] = result
        return data

    def run(self):
        try:
            with self.output().open("w") as f:
                data = dict(self.data)
                data = self.populate_cross_data(
                    data,
                    whitelist = self.whitelist
                )
                data = self.extract_data(data, "pays_fk/code_pays", "country")
                f.write(json.dumps(data))
            yield WriteToJSON(key=self.key)
        except Exception as e:
            self._handle_exception(data, e, f)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    rules_filepath = "./config/mouscron/fix-mouscron.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    mapping = {
        "type_list": "@type",
        "localite": "city",
        "numero": "number",
        "code_postal": "zipcode",
        "rue": "street",
        "mail": "email",
        "telephone": "phone",
        "matricule": "registrationNumber",
        "nom": "name1",
        "prenom": "name2",
        "numero_national": "nationalRegister",
        "societe": "society",
    }
    
    def remove_none_value(self, data):
        new_data = {}
        for key, value in data.items():
            if value is None or value == ".":
                continue
            new_data[key] = value
                
        return new_data

    def transform_data(self, data):
        data = super().transform_data(data)
        data = self.remove_none_value(data)
        return data
    
    def requires(self):
        return ValueCleanup(key=self.key)

   
class MappingCountry(core.MappingValueInMemoryTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    mapping_key="country"
    mapping = {
        "BE": "belgium",
        "FR": "france"
    }

    def requires(self):
        return Mapping(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    mapping_filepath = "./config/mouscron/mapping-type-mouscron-architecte.json"
    mapping_key = "@type"
    
    def requires(self):
        return MappingCountry(key=self.key)

class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    drop_keys = [
        'actif',
        'boite',
        'civilite2_fk',
        'civilite_fk',
        'cre_date',
        'cre_user',
        'fax',
        'gsm',
        'key',
        'langue',
        'mod_date',
        'mod_user',
        'numero_bce',
        'numero_bce_bad',
        'numero_national_bad',
        'pays_fk',
        "old_id"
    ]

    def requires(self):
        return MappingType(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "mouscron-architect"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/architect.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "mouscron-architect"
    export_filepath = "./result-mouscron-architect"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
