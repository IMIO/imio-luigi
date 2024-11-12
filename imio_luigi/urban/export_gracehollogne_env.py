# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools

import json
import logging
import luigi
import os
import re


MISSING_DECISION_FILE = "./data/gracehollogne/environements/missing_decision.txt"

logger = logging.getLogger("luigi-interface")


class GetJSON(core.GetFromAccessJSONTask):
    task_namespace = "gracehollogne_env"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)

    def _complete(self, key):
        """Method to speed up process that verify if output exist or not"""
        task = WriteToJSON(key=key)
        return task.complete()

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def handle_failure(self, data, errors):
        with self.log_failure_output().open("w") as f:
            error = {
                "error":  ", ".join(str(errors)),
                "data": data,
            }
            f.write(json.dumps(error))

    def run(self):
        min_range = None
        max_range = None
        counter = None
        if self.line_range:
            if not re.match("\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            min_range = int(line_range[0])
            max_range = int(line_range[1])
        if self.counter:
            counter = int(self.counter)
        iteration = 0
      
        for row in self.query(min_range=min_range, max_range=max_range):
            try:
                yield Transform(key=row["N°Permis"], data=row)
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
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def _make_title(self, data):
        data["title"] = f"{data['N°Permis']}"
        if "NatureEtablissement" in data:
            data["title"] += f" - {data['NatureEtablissement']}"
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        return data

    def run(self):
        with self.output().open("w") as f:
            data = core.utils.frozendict_to_dict(self.data)
            data = self._make_title(data)
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    rules_filepath = "./config/gracehollogne/fix-gracehollogne_env.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    mapping = {
        "N°Permis": "reference",
        "Catégorie": "@type",
        "NatureEtablissement": "licenceSubject",
        "Décision": "wf_transitions",
        "Arrêté": "ARRETE",
        "Datedemande": "DateDemande",
        "PV Enquête": "pv_enquete",
        "PVEnquête": "pv_enquete"
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMultiFormatMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    keys = (
        "DateDemande",
        "pv_enquete",
        "AvisCollège",
        "ARRETE",
        "ECHEANCE"
    )
    date_format_input = ["%m/%d/%y %H:%M:%S", "%d/%m/%y", "%d/%m/%Y"]
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    filepath = "./config/gracehollogne/add-data-gracehollogne.json"
    log_failure = False

    def log_failure_output(self):
        fname = self.key.replace("/", "-")
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
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

    def _get_data(self, data, keys):
        for key in keys:
            value = data.get(key, None)
            if value is not None:
                break
        return value

    def _check_validity_delay(self, data):
        echeance = data.get("ECHEANCE", None)
        if echeance is None:
            return data
        start = self._get_data(data, ["ARRETE", "AvisCollège", "pv_enquete", "DateDemande"])
        if start is None:
            return data
        try:
            echeance_date = datetime.fromisoformat(echeance)
            start_date = datetime.fromisoformat(start)
            data["validityDelay"] = echeance_date.year - start_date.year
        except ValueError as e:
            self._handle_exception(data, e)
        return data

    def transform_data(self, data):
        data = super().transform_data(data)
        data = self._check_validity_delay(data)
        return data

    def requires(self):
        return ConvertDates(key=self.key)


class MappingType(ucore.UrbanTypeMapping):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    mapping_filepath = "./config/gracehollogne/mapping-type-gracehollogne_env.json"
    mapping_key = "@type"
    log_failure = True

    def transform_data(self, data):
        if self.mapping_key in data:
            return super(MappingType, self).transform_data(data)
        data[self.mapping_key] = "MiscDemand"
        return data

    def requires(self):
        return AddExtraData(key=self.key)


class MappingDecision(core.MappingValueWithFileInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    mapping_filepath = "./config/gracehollogne/mapping-decision-gracehollogne_env.json"
    mapping_key = "wf_transitions"

    @property
    @core.utils._cache(ignore_args=True)
    def mapping(self):
        mapping = json.load(open(self.mapping_filepath, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    def _set_default_mapping(self, data):
        default_state = "accepted"
        data[self.mapping_key] = default_state
        with open(MISSING_DECISION_FILE, "a") as f:
            f.write(f"{data['reference']}\n")
        return data

    def transform_data(self, data):
        if self.mapping_key not in data:
            return self._set_default_mapping(data)
        if data[self.mapping_key] not in self.mapping:
            return self._set_default_mapping(data)

        data = super().transform_data(data)
        return data

    def requires(self):
        return MappingType(key=self.key)


class AddNISData(ucore.AddNISData):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()

    def requires(self):
        return MappingDecision(key=self.key)


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    log_failure = True
    ignore_event_missing_decision = False
    pv_avis_mapping = {
        "EnvClassOne": {"urban_type": "avis-college", "date": ["decisionDate", "eventDate"], "decision": "externalDecision"},
        "EnvClassTwo": {"urban_type": "avis-college", "date": ["decisionDate", "eventDate"], "decision": "externalDecision"},
        "EnvClassThree": {"urban_type": "acceptation-de-la-demande", "date": ["eventDate"]},
    }
    enquete = {
        "EnvClassOne": {"urban_type": "enquete-publique", "date": ["eventDate"]},
        "EnvClassTwo": {"urban_type": "avis-college", "date": ["eventDate"]},
        "MiscDemand": {"urban_type": "avis-college", "date": ["eventDate"]},
    }

    def requires(self):
        return AddNISData(key=self.key)

    def get_recepisse_check(self, data):
        return "DateDemande" in data and data.get("DateDemande", None) is not None

    def get_recepisse_date(self, data):
        return data.get("DateDemande", None)

    def get_delivery_check(self, data):
        return (
            "ARRETE" in data
            and data.get("ARRETE", None) is not None
            and "wf_transitions" in data
            and data.get("wf_transitions", None) is not None
        )

    def get_delivery_date(self, data):
        return data["ARRETE"]

    def get_delivery_decision(self, data):
        if data.get("wf_transitions") in ["accepted"]:
            decision = "favorable"
        elif data.get("wf_transitions") in ["refused", "inacceptable"]:
            decision = "defavorable"
        else:
            return None
        return decision

    def add_additonal_data_in_delivery(self, event, data):
        echeance = data.get("ECHEANCE", None)
        if echeance is not None:
            event["validityEndDate"] = echeance
        return event

    def _create_avis(self, data):
        if data["@type"] not in self.pv_avis_mapping:
            return data

        avis_college = data.get("AvisCollège", None)

        if avis_college is None:
            return data

        config = self.pv_avis_mapping.get(data["@type"], None)
        if config is None:
            return data

        urbaneventtypes = config["urban_type"]
        date_keys = config["date"]

        event = {
            "@type": "UrbanEvent",
            "urbaneventtypes": urbaneventtypes
        }

        for date_key in date_keys:
            event[date_key] = avis_college

        decision_key = config.get("decision", None)
        decision = self.get_delivery_decision(data)

        if decision_key is not None and decision is not None:
            event[decision_key] = decision

        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)

        return data

    def _create_enquete(self, data):
        if data["@type"] not in self.enquete:
            return data

        enquete_publique = data.get("pv_enquete", None)

        if enquete_publique is None:
            return data

        config = self.enquete.get(data["@type"], None)
        if config is None:
            return data

    def transform_data(self, data):
        data = super().transform_data(data)
        return data


class AddOtherEvent(ucore.AddEvents):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    event_config = {
        "pv_avis": {
            "check_key": ["AvisCollège"],
            "date_mapping": {"decisionDate": "AvisCollège", "eventDate": "AvisCollège"},
            "decision_mapping": {"accepted": "favorable", "refused": "defavorable", "inacceptable": "defavorable"},
            "mapping": {
                "EnvClassOne": {"urban_type": "copy_of_avis-college", "date": ["decisionDate", "eventDate"], "decision": "externalDecision"},
                "EnvClassTwo": {"urban_type": "avis-college", "date": ["decisionDate", "eventDate"], "decision": "externalDecision"},
                "EnvClassThree": {"urban_type": "acceptation-de-la-demande", "date": ["eventDate"]},
            }
        },
        "enquete": {
            "check_key": ["pv_enquete"],
            "date_mapping": {"investigationStart": "pv_enquete", "displayDate": "pv_enquete"},
            "mapping": {
                "EnvClassOne": {"@type": "UrbanEventInquiry", "urban_type": "copy_of_enquete-publique", "date": ["investigationStart"]},
                "EnvClassTwo": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart"]},
                "MiscDemand": {"urban_type": "enquete-publique", "date": ["displayDate"]},
            }
        }
    }

    def requires(self):
        return AddEvents(key=self.key)


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()

    def requires(self):
        return AddOtherEvent(key=self.key)


class UrbanTransitionMapping(ucore.UrbanTransitionMapping):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()

    def transform_data(self, data):
        wf_transititon = data.get("wf_transitions", None)
        if wf_transititon:
            data["wf_transitions"] = [wf_transititon]
        return super().transform_data(data)

    def requires(self):
        return EventConfigUidResolver(key=self.key)


class TransformApplicants(core.CreateSubElementInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    subelement_container_key = "applicants"
    mapping_keys = {
        "Demandeur": "Demandeur",
        "AdresseDemandeur": "AdresseDemandeur"
    }
    subelement_base = {}

    def requires(self):
        return UrbanTransitionMapping(key=self.key)


class CreateApplicant(ucore.CreateApplicant):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {
        "Demandeur": "name1",
        "AdresseDemandeur": "street"
    }
    subelement_base = {}

    def requires(self):
        return TransformApplicants(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    subelements_source_key = "SituationEtablissement"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = False

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def safe_convert_string_to_int(self, string):
        try:
            return int(string)
        except ValueError:
            return string

    def split_street_number(self, worklocation):
        worklocation = worklocation.replace("en la localité", "4460 GRACE-HOLLOGNE")
        worklocation = worklocation.replace("en l'entité", "4460 GRACE-HOLLOGNE")
        worklocation = worklocation.replace("\n", " ")
        worklocation = worklocation.replace("\r", " ")
        worklocation = worklocation.replace("  ", " ")

        # match zip and city
        city_pattern = r"(?P<rest>.*)(?P<zip>\d{4})\s+(?P<city>.*)"
        city_search = re.search(city_pattern, worklocation.strip())

        if city_search is None:
            result = {
                'rest': worklocation,
                "zip": None,
                "city": None,
            }
        else:
            result = city_search.groupdict()

        # match street number
        number_pattern = r"(?P<rest>\D*(?:n°)?\D)(?P<number>\d*(?:[A-Za-z\\/\s-]|(?:bte))*\d*)"
        number_search = re.search(number_pattern, result["rest"].strip())

        if number_search:
            number_result = number_search.groupdict()
            result["number"] = number_result["number"]
            result["rest"] = number_result["rest"]

        # match street name
        street_pattern = r"^(?P<street>.*?)(?=\s*(?:n°|à|bte|,|$))"
        street_search = re.search(street_pattern, result["rest"].strip())

        if street_search:
            street_result = street_search.groupdict()
            result["street"] = street_result["street"]

        return result

    def transform_data(self, data):
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
        if self.subelements_source_key not in data:
            if self.ignore_missing is False:
                raise KeyError(f"Missing key {self.subelements_source_key}")
            return data
        if (
            data[self.subelements_source_key] == "-"
            or data[self.subelements_source_key] == ""
            or data[self.subelements_source_key] is None
        ):
            raise KeyError("Pas d'adresse présente")
        worklocation = data[self.subelements_source_key]
        data = utils.add_data_to_description(data, f"Adresse : {worklocation}\n")
        new_element = self.split_street_number(worklocation)
        data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformWorkLocation(ucore.TransformWorkLocationMultiParams):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    log_failure = True
    report_path = "./data/gracehollogne/environements/report_work_location.txt"

    def requires(self):
        return CreateWorkLocation(key=self.key)

    def generate_term(self, worklocation, shapes):
        terms = []
        for shape in shapes:
            value = worklocation.get(shape, None)
            if value:
                terms.append(value)
        return " ".join(terms)

    def _generate_params(self, worklocation, data):
        params_shape = [
            ["street", "zip", "city"],
            ["rest", "zip", "city"],
        ]
        return [
            {
                "key": "term",
                "value": self.generate_term(worklocation, shape)
            }
            for shape in params_shape
        ], None

    def get_range_number(self, number):
        number_list = number.split("-")
        return int(number_list[0]), int(number_list[1])

    def transform_data(self, data):
        data, errors = super().transform_data(data)
        if "workLocations" not in data or len(data["workLocations"]) < 1:
            return data, errors
        worklocation = data["workLocations"][0]
        number = worklocation.get("number", None)
        if not number:
            return data, errors
        if "-" not in number:
            return data, errors
        start, end = self.get_range_number(worklocation["number"])
        data["workLocations"] = [
            {
                "street": worklocation["street"],
                "number": str(i)
            }
            for i in range(start, end)
        ]
        return data, errors


class ExtractDivAndSecFromCadastre(core.InMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()

    def transform_data(self, data):
        if "cadastre" not in data:
            return data
        cadastre = data.get('cadastre', None)
        if not cadastre:
            return data
        pattern = r"(?P<division>\d)(?:ème\s|ère\s|\s)division, [sS]ection\s?(?P<section>[A-E])(?:,\sn°|[,\s]*)(?P<cadastre>.*)"
        match = re.match(pattern, cadastre)
        if not match:
            return data
        result = match.groupdict()
        if "division" in result:
            data["division"] = result["division"]
        if "section" in result:
            data["section"] = result["section"]
        if "cadastre" in result:
            data["cadastre"] = result["cadastre"]
        return data

    def requires(self):
        return TransformWorkLocation(key=self.key)


class CadastreSplit(core.StringToListRegexpInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", "et", "-"]

    def requires(self):
        return ExtractDivAndSecFromCadastre(key=self.key)


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    log_failure = True
    mapping_division_dict = {
        "1": "52018",
        "2": "52054",
    }

    def requires(self):
        return CadastreSplit(key=self.key)

    def _fix_lower_chars(self, params):
        output = {}
        for key, value in params.items():
            if type(value) is str:
                output[key] = value.upper()
        return output

    def _generate_cadastre_dict(self, cadastre, data):
        try:
            params = tools.extract_cadastre(cadastre)
        except ValueError:
            return None, f"Valeur incorrecte pour la parcelle: {cadastre}"
        if "division" in data:
            params["division"] = self._mapping_division(data["division"])
        if "section" in data:
            params["section"] = data["section"]

        return self._fix_lower_chars(params), None


class AddValuesInDescription(ucore.AddValuesInDescription):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    title = "Info complémentaire"
    key_to_description = [
        "N°DP",
        "pv_enquete",
        "ARRETE",
        "ECHEANCE",
        "N°Registre",
        "Arrêté",
        "Remarques",
        "SituationEtablissement"
    ]
    key_dict = {
        "ARRETE": "Arrêté",
        "ECHEANCE": "Échéance",
        "SituationEtablissement": "Situation Etablissement"
    }
    keys_date = [
        "pv_enquete",
        "ARRETE",
        "ECHEANCE",
        "Arrêté",
    ]

    def fix_key(self, key):
        return self.key_dict.get(key, key)

    def get_values(self, data):
        result = []
        for key, value in data.items():
            if key in self.key_to_description and value is not None:
                result.append(
                    {"key": key, "value": value}
                )
        return result

    def handle_value(self, value, data):
        key = value["key"]
        value = value["value"]
        data["description"]["data"] += f"{self.fix_key(key)} : {value}"
        return data

    def requires(self):
        return TransformCadastre(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    drop_keys = [
        'adresse',
        'annee',
        'cadastre',
        'date de decision',
        'demandeur',
        'etat',
        'procedure',
        'ARRETE',
        'AdresseDemandeur',
        'AvisCollège',
        'DateDemande',
        'Demandeur',
        'ECHEANCE',
        'N°DP',
        'N°Registre',
        'pv_enquete',
        'SituationEtablissement',
        'Remarques',
        'Arrêté',
        'Datedemande',
        "applicants"
    ]

    def requires(self):
        return AddValuesInDescription(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "gracehollogne_env"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "gracehollogne_env"
    export_filepath = "./result-gracehollogne-env"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
