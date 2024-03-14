# -*- coding: utf-8 -*-

from imio_luigi import core
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools

import json
import logging
import luigi
import re


logger = logging.getLogger("luigi-interface")


class GetFromCSV(core.GetFromCSVFile):
    task_namespace = "flemalle"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    delimiter = ";"
    dtype = "string"

    def _complete(self, key):
        """Method to speed up process that verify if output exist or not"""
        task = WriteToJSON(key=key)
        return task.complete()

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
        # res = get_all_unique_value(self.query(), "Statut du dossier")
        for row in self.query(min_range=min_range, max_range=max_range):
            try:
                yield Transform(key=row["Noref"], data=row)
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
    task_namespace = "flemalle"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    log_failure = True

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
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

    def _parcel_in_description(self, data):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        data["description"]["data"] += (
            f"<p>Cadastre :</p>\r\n"
            f"<p>Div: {data.get('Div', '')}</p>\r\n"
            f"<p>Sec: {data.get('Sec', '')}</p>\r\n"
            f"<p>Nu2a: {data.get('Nu2a', '')}</p>\r\n"
            f"<p>Nu2b: {data.get('Nu2b', '')}</p>\r\n"
        )
        return data

    def run(self):
        try:
            with self.output().open("w") as f:
                data = dict(self.data)
                data = self._parcel_in_description(data)
                data["title"] = data["Noref"]
                f.write(json.dumps(data))
            yield WriteToJSON(key=self.key)
        except Exception as e:
            self._handle_exception(data, e, f)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    rules_filepath = "./config/flemalle/fix-flemalle-notaire.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    mapping = {
        "Noref": "reference"
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    keys = ("Dce")
    date_format_input = "%Y-%m-%d %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    filepath = "./config/flemalle/add-data-flemalle.json"

    def requires(self):
        return ConvertDates(key=self.key)


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    create_delivery = False

    def requires(self):
        return AddExtraData(key=self.key)

    def get_recepisse_check(self, data):
        return "Dce" in data or data["Dce"]

    def get_recepisse_date(self, data):
        return data.get("Dce", None)


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "flemalle"
    key = luigi.Parameter()

    def requires(self):
        return AddEvents(key=self.key)


class TransformNotary(ucore.TransformContact):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    contact_type = "notaries"
    data_key = "notaryContact"
    log_failure = True

    def requires(self):
        return EventConfigUidResolver(key=self.key)

    def _remove_dot(self, name):
        return name.rstrip(".")

    def _generate_contact_name(self, data):
        term = ""
        name = data.get("Nom", None)
        if name:
            term += self._remove_dot(name)
        fname = data.get("Prenom", None)
        if fname and name:
            term += f" {self._remove_dot(fname)}"
        if fname and not name:

            term += f"{self._remove_dot(fname)}"
        if term == "":
            return None, "Pas de notaire"

        return term, None


class Nu2aCadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    attribute_key = "Nu2a"
    separators = ["/", "-"]

    def _recursive_split(self, value, separators):
        regexp = f"[{''.join(separators)}]"
        return [v for v in re.split(regexp, value) if v and v not in separators]

    def requires(self):
        return TransformNotary(key=self.key)


class Nu2bCadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    attribute_key = "Nu2b"
    separators = ["/", "-"]

    def _recursive_split(self, value, separators):
        regexp = f"[{''.join(separators)}]"
        return [v for v in re.split(regexp, value) if v and v not in separators]

    def requires(self):
        return Nu2aCadastreSplit(key=self.key)


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    log_failure = True
    mapping_division_dict = {
        "5": "62007",
        "9": "62036",
        "1": "62037",
        "7": "62055",
        "8": "62072",
        "3": "62083",
        "6": "62402",
        "4": "62412",
        "2": "62502"
    }

    def _mapping_division(self, value):
        if value not in self.mapping_division_dict:
            return "99999"
        return self.mapping_division_dict[value]

    def requires(self):
        return Nu2bCadastreSplit(key=self.key)

    def _handle_Nu2b_partie(self, Nu2b, cadastres):
        if Nu2b[0].lower() == "parties" or Nu2b[0].lower() == "partie" or Nu2b[0].lower() == "pie" or Nu2b[0].lower() == "pies":
            new_cadastre = []
            for cadastre in cadastres:
                new_cadastre.append(f"{cadastre} {Nu2b[0]}")
            return new_cadastre
        return cadastres + Nu2b

    def transform_data(self, data):
        Nu2a = data.get("Nu2a", None)
        Nu2b = data.get("Nu2b", None)
        cadastre = None
        if Nu2a:
            cadastre = Nu2a
        if Nu2b:
            cadastre = self._handle_Nu2b_partie(Nu2b, cadastre)
        if cadastre:
            data["cadastre"] = cadastre
        return super().transform_data(data)

    def _check_partie(self, params):
        partie = params.get("partie", None)
        if partie:
            params["partie"] = True
        return params

    def _generate_cadastre_dict(self, cadastre, data):
        if cadastre == "Non cadastré":
            return None, ""
        pattern = rf"(?P<radical>\d{{0,4}})\/?(?P<bis>\d{{0,2}})\s*(?P<exposant>[a-zA-Z]?)\s*(?P<puissance>\d{{0,2}})\s*(?P<partie>(?:{tools.PARTIES_REGEXP}){{0,1}})"
        cadastre_split = re.match(pattern, cadastre.strip())
        if not cadastre_split:
            msg = f"Impossible de reconnaitre la parcelle '{cadastre}'"
            return None, msg
        params = cadastre_split.groupdict()
        params = self._check_partie(params)
        division = data.get('Div', None)
        if division:
            params["division"] = self._mapping_division(division)
        section = data.get('Sec', None)
        if section:
            params["section"] = section
        params["browse_old_parcels"] = True
        return params, None


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    subelements_source_key = "Coderue"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = True

    def transform_data(self, data):
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
        if self.subelements_source_key not in data:
            if self.ignore_missing is False:
                raise KeyError(f"Missing key {self.subelements_source_key}")
            return data

        new_element = {
            "street": data["Coderue"],
        }
        number = data.get('Nuo', "")
        if number is not None:
            new_element["number"] = number
        else:
            new_element["number"] = ""
        data[self.subelements_destination_key] = [new_element]
        return data

    def requires(self):
        return TransformCadastre(key=self.key)


class TransformWorkLocation(ucore.TransformWorkLocation):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    street_json = "./data/flemalle/json/Rues.json"

    @property
    def get_street_dict(self):
        with open(self.street_json, "r") as f:
            output = {
                str(json.loads(item)["coderue"]):json.loads(item)["libellerue"]
                for item in f.readlines()
            }
        return output

    def _generate_street_code(self, worklocation, data):
        street = worklocation.get("street", None)
        if not street:
            return None, "Pas de nom de rue présent"
        return street, None

    def _handle_failed_street_code(self, worklocation, data):
        street_code, error = self._generate_street_code(worklocation, data)
        street_name = self.get_street_dict.get(street_code, None)
        if street_name is None:
            return None, f"Pas de nom rue trouvé pour {street_code}"
        params = {"match": self.search_match, "include_disable": self.seach_disable}
        params["term"] = street_name
        r = self.request(parameters=params)
        if r.status_code != 200:
            return None, f"Response code is '{r.status_code}', expected 200"
        return r.json(), None

    def requires(self):
        return CreateWorkLocation(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    drop_keys = [
        'CodePostal',
        'Coderue',
        'Dce',
        'Div',
        'Nom',
        'Noref',
        'Nu2a',
        'Nu2b',
        'Nuo',
        'Prenom',
        'Sec',
        'Tp',
        'Unnamed: 0',
        'cadastre',
        'key'
    ]

    def requires(self):
        return TransformWorkLocation(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "flemalle"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "flemalle"
    export_filepath = "./result-flemalle-notaries"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
