# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools

import json
import logging
import luigi
import os
import re


logger = logging.getLogger("luigi-interface")


class GetFromAccess(core.GetFromAccessJSONTask):
    task_namespace = "lierneux"
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
                "error": ", ".join(str(errors)),
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
        # res = utils.get_all_unique_value(self.query(),"ArchiAdresse")
        # res = utils.get_all_keys(self.query())
        # __import__('pdb').set_trace()
        # res = [row.get("Numero", "---No Ref---") for row in self.query()]
        for row in self.query(min_range=min_range, max_range=max_range):
            # with open("./data/lierneux/all_ref_to_import.txt", "a") as f:
            #     f.write(f"{row.get('Numero', '---No Ref---')}\n")
            if "TYPE" not in row:
                continue
            try:
                yield Transform(key=row["REF"], data=row)
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
    task_namespace = "lierneux"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            data = core.utils.frozendict_to_dict(self.data)
            data["title"] = data["REF"]
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)



class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    rules_filepath = "./config/lierneux/fix-lierneux.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

class AddRelation(core.RelationAcessInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    config_dict = {
        "encours": {
            "file": "./data/lierneux/json/Urbi7/enoucrs.json",
            "fk": "REF",
            "pk": "ref_dossier",
            "relation": "one_to_many",
            "_actions": {
                "file": "./data/lierneux/json/Urbi7/ACTIONS.json",
                "fk": "ref_action",
                "pk": "REF",
                "relation": "one_to_one",
                "_liens": {
                    "file": "./data/lierneux/json/Urbi7/ActionsLiens.json",
                    "fk": "REF",
                    "pk": "ref",
                    "relation": "many_to_many",
                    "bound": {
                        "file": "./data/lierneux/json/Urbi7/ACTIONS.json",
                        "fk": "appel",
                        "pk": "REF",
                        "relation": "one_to_one"
                    }
                }
            }
        },
        "DGATLP": {
            "file": "./data/lierneux/json/Urbi7/InfosDGATLP.json",
            "fk": "REF",
            "pk": "REF",
            "relation": "one_to_one"
        }
    }

class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    mapping = {
        "REF": "reference",
        "TYPE": "@type",
        "CADASTRE": "cadastre",
        "OBJET": "licenceSubject"
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    keys = ("RECEPT", "CLOTURE", "TravauxDebut", "TravauxFin", "RefBatiDate", "RefLotDate", "EcheanceDate")
    date_format_input = "%m/%d/%y %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


# todo: Check agent traitant
class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    filepath = "./config/lierneux/add-data-lierneux.json"

    def requires(self):
        return ConvertDates(key=self.key)


class MappingType(ucore.UrbanTypeMapping):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    mapping_filepath = "./config/lierneux/mapping-type-lierneux.json"
    mapping_key = "@type"

    def get_date(self, data):
        return data.get("RECEPT", None)

    def requires(self):
        return AddExtraData(key=self.key)


class AddNISData(ucore.AddNISData):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def requires(self):
        return MappingType(key=self.key)


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    log_failure = False
    override_event= {
        "recepisse" : {
            "CODT_CommercialLicence" : ["depot-de-la-demande-codt", "UrbanEvent"],
            "CODT_UrbanCertificateTwo" : ["depot-de-la-demande-codt", "UrbanEvent"]
        },
        "delivery" : {
            "CODT_UrbanCertificateTwo" : ["copy_of_delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"]
        }
    }

    def requires(self):
        return AddNISData(key=self.key)

    def get_recepisse_check(self, data):
        return "RECEPT" in data

    def get_recepisse_date(self, data):
        return data["RECEPT"]

    def get_delivery_check(self, data):
        columns = ("CLOTURE", "refuse")
        matching_columns = [c for c in columns if c in data]
        return matching_columns

    def get_delivery_date(self, data):
        return data["CLOTURE"]

    def get_delivery_decision(self, data):
        if data.get("refuse") == 0:
            decision = "favorable"
        elif data.get("refuse") == 1:
            decision = "defavorable"
        elif data.get("refuse") in [-1, 2]:
            decision = None
        else:
            raise ValueError("Pas de valeur correct pour la decisison de cloture")
        return decision


class AddOtherEvent(ucore.AddEvents):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    event_config = {
        "start_work": {
            "check_key": ["TravauxDebut"],
            "date_mapping": {"eventDate": "TravauxDebut"},
            "mapping": {
                "Article127": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "BuildLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_BuildLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_CommercialLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_ParcelOutLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_UrbanCertificateOne": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_UrbanCertificateTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "EnvClassTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "MiscDemand": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "UniqueLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateOne": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
                "UrbanCertificateTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},  # todo
            }
        },
        "end_work": {
            "check_key": ["Fin_enquete"],
            "date_mapping": {"eventDate": "TravauxFin"},
            "mapping": {
                "Article127": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "BuildLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_BuildLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_CommercialLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_ParcelOutLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_UrbanCertificateOne": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "CODT_UrbanCertificateTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "EnvClassTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "MiscDemand": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "UniqueLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateOne": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
                "UrbanCertificateTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},  # todo
            }
        }
    }

    def requires(self):
        return AddEvents(key=self.key)


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def requires(self):
        return AddOtherEvent(key=self.key)


class AddTransitions(core.InMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def requires(self):
        return EventConfigUidResolver(key=self.key)

    def transform_data(self, data):
        state = None
        if data.get("refuse") == 0:
            state = "accepted"
        elif data.get("refuse") == 1:
            state = "refused"
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class UrbanTransitionMapping(ucore.UrbanTransitionMapping):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def requires(self):
        return AddTransitions(key=self.key)


class CreateApplicant(ucore.CreateApplicant):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {
        "PROPRIET": "name1",
        "COMMUNE": "city",
        "CODE": "zipcode",
        "ADRESSE": "street",
    }
    subelement_base = {}

    def requires(self):
        return UrbanTransitionMapping(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    subelements_source_key = "AdresseBien"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = True

    def safe_convert_string_to_int(self, string):
        try:
            return int(string)
        except ValueError:
            return string

    def split_street_number(self, worklocation):
        pattern = r"^(?P<street>.*?)(?:,?\s?(?P<number>\d*(?:[\/-]|\b(?:bte|[a-zA-Z])\b)?\d*)?)?$"
        match = re.match(pattern, worklocation.strip())
        if not match:
            raise ValueError(f"Ne trouve aucun pattern de rue dans {worklocation}")
        result = match.groupdict()
        if (
            "number" in result
            and len(result["number"]) == 1
            and type(self.safe_convert_string_to_int(result["number"])) is str
        ):
            result["street"] += result["number"]
            result["number"] = ""
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
        worklocation = data[self.subelements_source_key]
        new_element = self.split_street_number(worklocation)
        data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformWorkLocation(ucore.TransformWorkLocation):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CreateWorkLocation(key=self.key)

    def _generate_term(self, worklocation, data):
        return worklocation.get("street", None), None

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
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def transform_data(self, data):
        if "cadastre" not in data:
            return data
        cadastre = data.get('cadastre', None)
        if not cadastre:
            return data
        pattern = r"(?P<division>\d)\s*(?:ème|ère|\s|e|°)\s*(?:[dD]ivision|div.?|vision|divisision|dv.),? (?:[sS]ection|[Ss]ect.)\s*(?P<section>[A-Za-z])\s*;?,?\s*(?:devant)?\s*(?:la parcelle|les)?\s*(?:n\s?°|[,\s]*|;|N|N n°|N\s?°|n)\s*(?P<cadastre>\d.*)"
        match = re.match(pattern, cadastre)
        if not match:
            return data
        result = match.groupdict()
        # if self.key == "2000/U029":
        #     # todo: PDB to remove before commit
        #     __import__('pdb').set_trace()
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
    task_namespace = "lierneux"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", "et", "-", "&"]

    def requires(self):
        return ExtractDivAndSecFromCadastre(key=self.key)


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    log_failure = True
    mapping_division_dict = {
        "1": "63045",
        "2": "63011",
        "3": "63343",
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


class TransformArchitect(ucore.TransformContact):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    log_failure = True
    contact_type = "architects"
    data_key = "architects"

    def requires(self):
        return TransformCadastre(key=self.key)

    def _fix_term(self, term):
        term = term.strip()
        term = term.replace("(", " ")
        term = term.replace(")", " ")
        term = term.replace("  ", " ")
        return term

    def _generate_contact_name(self, data):
        if "ARCHI" not in data:
            return None, None
        if data["ARCHI"].strip() == "":
            return None, None
        return self._fix_term(data["ARCHI"]), None


class AddValuesInDescription(ucore.AddValuesInDescription):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    title = "Info complémentaire"
    key_to_description = [
        'EcheanceAction',
        'EcheanceDate',
        'EcheanceStatus',
        'Auteur_projet',
        'AvisFonctionnaire'
    ]
    key_dict = {
        'EcheanceAction': 'Echeance action',
        'EcheanceDate': 'Echeance date',
        'EcheanceStatus': 'Echeance status',
        'Auteur_projet' : "Auteur du projet",
        'AvisFonctionnaire' : "Avis du fonctionnaire"
    }
    
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
        return TransformArchitect(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    drop_keys = [
        'ADRESSE',
        'AdresseBien',
        'ARCHI',
        'ArchiAdresse',
        'ArchiCode',
        'ArchiCommune',
        'cadastre',
        'CadastreType',
        'Civilite',
        'CLOTURE',
        'CODE',
        'COMMUNE',
        'complet',
        'division',
        'EcheanceAction',
        'EcheanceDate',
        'EcheanceStatus',
        'FORMTYPE',
        'gestUrba',
        'MANDANT',
        'MODELE',
        'PROPRIET',
        'RECEPT',
        'REF_CAD',
        'RefBatiDate',
        'RefBatiNo',
        'refcloture',
        'RefLotDate',
        'refuse',
        'section',
        'TaxeCommunale',
        'TravauxDebut',
        'TravauxFin',
        "RefLotNo",
        'urba',
        'NumPermis',
        'AvisFonctionnaire',
        'ArchiFax',
        'ArchiTel',
        'Auteur_adresse',
        'Auteur_code',
        'Auteur_commune',
        'Auteur_projet'
    ]

    def requires(self):
        return AddValuesInDescription(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "lierneux"
    export_filepath = "./results/result-lierneux"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
