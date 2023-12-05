# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban.address import find_address_match
from imio_luigi.urban import tools

import json
import logging
import luigi
import numpy as np
import os
import pandas as pd
import re
import copy

logger = logging.getLogger("luigi-interface")


def get_db_data(key):
    key_index = 0
    match = re.match(r"(\d{4}-[\dA-Za-z-\s,]+)(_?(\d)*)", key)
    if match:
        key = match.group(1)
        index = match.group(3)
        if index:
            key_index = int(index) - 1

    db_path = "./data/gracehollogne/permis.csv"
    db_data = pd.read_csv(
        db_path,
        delimiter=',',
        keep_default_na=True,
        na_values=[None]
    )
    licences = db_data.loc[db_data["NUM_PERMIS"] == key]

    output = licences.iloc[key_index].replace({np.nan: None}).to_dict()

    return output


class GetFromAccess(core.GetFromAccessJSONTask):
    task_namespace = "gracehollogne"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    

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
        for row in self.query(min_range=min_range, max_range=max_range):
            try:
                yield Transform(key=row["référence"], data=row)
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
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def _generate_title(self, data):
        data["title"] = f"{data['référence']} - {data['objet']}"
        return data

    def _reset_description(self, data):
        if not "description" in data:
            return data
        data["description"] = {
            "data": "",
            "content-type": "text/html"
        }
        return data

    def run(self):
        with self.output().open("w") as f:
            data = core.frozendict_to_dict(self.data)
            data = self._generate_title(data)
            data = self._reset_description(data)
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    rules_filepath = "./config/gracehollogne/fix-gracehollogne.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

class AddTransitions(core.InMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    
    @property
    def db_data(self):
        return get_db_data(self.key)
    
    def requires(self):
        return ValueCleanup(key=self.key)

    def transform_data(self, data):
        refused = self.db_data.get("REFUS", None)
        accepted = self.db_data.get("AUTORISATION", None)
        if accepted:
            data["wf_transitions"] = ["accepted"]
        elif refused:
            data["wf_transitions"] = ["refused"]
        else:
            data["wf_transitions"] = ["deposit"]
        return data


class AddNISData(tools.AddNISData):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()

    def requires(self):
        return AddTransitions(key=self.key)

class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    mapping = {
        "référence": "reference",
        "objet": "licenceSubject",
        "n°_cadastral": "cadastre",
        "référence DGATLP": "referenceDGATLP",
        "adresse des travaux": "workLocations"
    }

    def requires(self):
        return AddNISData(key=self.key)


class FixSubObjectKey(core.MappingKeysInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    mapping = {
        "Architect": {
            "civilité": "personTitle",
            "nom": "name1",
            "prénom": "name2",
            "téléphone": "phone",
            "mobile": "gsm",
            "rue": "street",
            "code postal": "zipcode",
            "localité": "city",
            "pays": "country",
            "numéro": "number"
        },
        "workLocations": {
            "numéro": "number",
            "rue": "street",
            "code postal": "zipcode"
        }
    }
    key_with_list = ["architects", "__children__", "workLocations"]

    def transform_data(self, data):
        """Transform data with the mapping table"""
        for key in self.key_with_list:
            if key not in data:
                continue
            for item in data[key]:
                item_type = item.get("@type", key)
                if item_type not in self.mapping:
                    continue
                for original, destination in self.mapping[item_type].items():
                    if self.ignore_missing is True:
                        if original in item:
                            item[destination] = item[original]
                            del item[original]
                    else:
                        item[destination] = item[original]
                        del item[original]
        return data

    def requires(self):
        return Mapping(key=self.key)


class ClearEventAndApplicant(core.InMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    type_to_remove = {
        "__children__": {
            "UrbanEvent": {
                "event_id": "delivrance-du-permis-octroi-ou-refus"
            },
            "Applicant": {}}
    }

    def requires(self):
        return FixSubObjectKey(key=self.key)

    def transform_data(self, data):
        for key in self.type_to_remove:
            new_data = []
            for item in data[key]:
                truth_list = []
                for type in self.type_to_remove[key]:
                    if item["@type"] == type:
                        truth_list.append(False)
                    else:
                        truth_list.append(True)
                    for item_key, value in self.type_to_remove[key][type].items():
                        if item_key in item and item[item_key] == value:
                            truth_list.append(False)
                        else:
                            truth_list.append(True)
                if all(truth_list):
                    new_data.append(item)
            data[key] = new_data
        return data


class AddEvents(core.InMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    date_format_input = "%m/%d/%y %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"

    @property
    def db_data(self):
        return get_db_data(self.key)

    def requires(self):
        return ClearEventAndApplicant(key=self.key)

    def transform_data(self, data):
        # data = self._create_recepisse(data)
        data = self._clear_old_recepisse_event(data)
        data = self._create_delivery(data)
        return data

    def _convert_date(self, date_input):
        date = datetime.strptime(date_input, self.date_format_input)
        return date.strftime(self.date_format_output)

    def _clear_old_recepisse_event(self, data):
        children = []
        for item in data["__children__"]:
            if not (
                "@type" in item
                and "event_id" in item
                and item["@type"] == "UrbanEvent"
                and item["event_id"] == "delivrance-du-permis-octroi-ou-refus"
            ):
                children.append(item)
        data["__children__"] = children
        return data

    def _create_recepisse(self, data):
        """Create recepisse event"""
        if "entrée" not in data:
            return data
        event_subtype, event_type = self._mapping_recepisse_event(data["@type"])
        event = {
            "@type": event_type,
            "eventDate": data["entrée"],
            "urbaneventtypes": event_subtype,
        }
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _create_delivery(self, data):
        columns = ("REFUS", "AUTORISATION")
        matching_columns = [c for c in columns if c in self.db_data]
        if not matching_columns:
            return data
        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        if self.db_data.get("AUTORISATION"):
            decision = "favorable"
            date = self.db_data.get("AUTORISATION")
        elif self.db_data.get("REFUS"):
            decision = "defavorable"
            date = self.db_data.get("REFUS")
        else:
            return data
        event = {
            "@type": event_type,
            "decision": decision,
            "decisionDate": self._convert_date(date),
            "urbaneventtypes": event_subtype,
        }
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _mapping_recepisse_event(self, type):
        data = {
            "BuildLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_BuildLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "CODT_Article127": ("reception-demande-dgo4-codt", "UrbanEvent"),
            "Article127": ("depot-de-la-demande", "UrbanEvent"),
            "IntegratedLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_IntegratedLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "Declaration": ("depot-de-la-demande", "UrbanEvent"),
            "UrbanCertificateOne": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateOne": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UrbanCertificateTwo": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateTwo": ("depot-de-la-demande", "UrbanEvent"),
            "PreliminaryNotice": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassOne": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassTwo": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassThree": ("depot-de-la-demande", "UrbanEvent"),
            "ParcelOutLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "MiscDemand": ("depot-de-la-demande", "UrbanEvent"),
            "NotaryLetter": ("depot-de-la-demande", "urbanEvent"),
        }
        return data[type]

    def _mapping_delivery_event(self, type):
        data = {
            "BuildLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_BuildLicence": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "CODT_Article127": (
                "permis-decision-fd-codt",
                "UrbanEvent",
            ),
            "CODT_UrbanCertificateOne": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "CODT_UrbanCertificateTwo": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "Article127": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"),
            "Declaration": ("deliberation-college", "UrbanEvent"),
            "UrbanCertificateOne": ("octroi-cu1", "UrbanEvent"),
            "UrbanCertificateTwo": ("octroi-cu2", "UrbanEvent"),
            "UniqueLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_UniqueLicence": ("delivrance-permis", "UrbanEvent"),
            "MiscDemand": ("deliberation-college", "UrbanEvent"),
            "EnvClassOne": ("decision", "UrbanEvent"),
            "EnvClassTwo": ("decision", "UrbanEvent"),
            "EnvClassThree": ("passage-college", "UrbanEvent"),
            "PreliminaryNotice": ("passage-college", "UrbanEvent"),
            "NotaryLetter": ("octroi-lettre-notaire", "UrbanEvent"),
        }
        return data[type]


class EventConfigUidResolver(tools.UrbanEventConfigUidResolver):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    
    def requires(self):
        return AddEvents(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    filepath = "./config/gracehollogne/add-data-gracehollogne.json"

    def requires(self):
        return EventConfigUidResolver(key=self.key)


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    log_failure = True

    @property
    def db_data(self):
        return get_db_data(self.key)

    def requires(self):
        return AddExtraData(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/@address"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def _prepare_term(self, worklocation):
        output = {"term": ""}
        if "street" in worklocation:
            output["term"] += worklocation["street"]
        if "locality" in worklocation:
            output["term"] += f" ({worklocation['zipcode']} - {worklocation['locality']})"
        return output

    def _get_adress_db(self):
        street = self.db_data["SITUATION_BIEN"]
        if not street:
            return []
        seperator = r"(?<=[\d,])\s+"
        match_locations = re.split(seperator, street)
        output = []
        if not match_locations:
            return []
        for location in match_locations:
            match_street = re.match(r'(\D+)\s+(\d+)', location)
            if not match_street:
                output.append({"street": location})
                continue
            if len(match_street.groups()) > 1:
                output.append({
                    "street": match_street.group(1),
                    "number": match_street.group(2)
                })
            else:
                output.append({"street": location})
        return output

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        workLocations = self._get_adress_db()
        for worklocation in workLocations:
            params = self._prepare_term(worklocation)
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
                desc_data = data.get('description').get('data')
                if "Pas de résultat pour cette rue" not in desc_data:
                    errors.append(f"Aucun résultat pour l'adresse: '{params['term']}'")
                continue
            elif result["items_total"] > 1:
                match = find_address_match(result["items"], worklocation["street"])
                if not match:
                    errors.append(
                        f"Plusieurs résultats pour l'adresse: '{params['term']}'"
                    )
                    continue
            else:
                match = result["items"][0]

            obj = {"street": match["uid"]}
            if worklocation.get("number"):
                obj["number"] = worklocation.get("number")
            new_work_locations.append(obj)

        data["workLocations"] = new_work_locations
        return data, errors


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return TransformWorkLocation(key=self.key)

    @property
    def db_data(self):
        return get_db_data(self.key)

    @property
    def request_url(self):
        return f"{self.url}/@parcel"

    def _mapping_division(self, data):
        mapping = {
            "1": "62046",
            "2": "62054",
            "3": "62453",
            "4": "62454",
            "5": "62105",
            "6": "62016",
        }
        if 'division' not in data or data['division'] not in mapping:
            return "99999"
        return mapping[data['division']]

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            # cleanup
            error = error.replace(", 'browse_old_parcels': True", "")
            error = error.replace("'", "")
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def _prepare_cadastre(self):
        number = self.db_data["N"]
        if not number:
            return []
        return number.split(",")

    def transform_data(self, data):
        errors = []
        for cadastre in self._prepare_cadastre():
            params = {
                "division": self.db_data["DIV"],
                "section": self.db_data["SECT"]
            }
            cadastre_split = re.match(r"(\d{0,4})([A-Za-z_]*)(\d{0,3})", cadastre.strip())
            part_order = ("radical", "exposant", "puissance")
            if not cadastre_split:
                continue
            for count, part in enumerate(cadastre_split.groups()):
                params[part_order[count]] = part
            params["division"] = self._mapping_division(params)
            params["browse_old_parcels"] = True
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
                del params["browse_old_parcels"]
                errors.append(f"Aucun résultat pour la parcelle '{params}'")
                continue
            elif result["items_total"] > 1:
                del params["browse_old_parcels"]
                errors.append(f"Plusieurs résultats pour la parcelle '{params}'")
                continue
            if "__children__" not in data:
                data["__children__"] = []
            new_cadastre = result["items"][0]
            new_cadastre["@type"] = "Parcel"
            if "capakey" in new_cadastre:
                new_cadastre["id"] = new_cadastre["capakey"].replace("/", "_")
            if "old" in new_cadastre:
                new_cadastre["outdated"] = new_cadastre["old"]
            else:
                new_cadastre["outdated"] = False
            for key in ("divname", "natures", "locations", "owners", "capakey", "old"):
                if key in new_cadastre:
                    del new_cadastre[key]
            data["__children__"].append(new_cadastre)
        return data, errors
    

class CreateApplicant(core.CreateSubElementInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {}
    subelement_base = {"@type": "Applicant"}

    @property
    def db_data(self):
        return get_db_data(self.key)

    def _get_street_number(self):
        street = self.db_data.get("ADRESSE", "")
        if not street:
            return None, None

        match = re.search(r"^(.*?)(?:\s*(\d+)\s*)?$", street.strip())
        if match and len(match.groups()) > 1:
            output_street = match.group(1).strip()
            output_number = None
            if match.group(2):
                output_number = match.group(2).strip()
            return output_number, output_street

        return street, None

    def transform_data(self, data):
        applicant = self.db_data.get("NOM", None)
        if not applicant:
            return data

        subelement_base = {"@type": "Applicant"}
        subelement_base["name1"] = applicant
        data["title"] = f"{data['title']} - {applicant}"
        name2 = self.db_data.get("PRENOM", None)
        if name2:
            subelement_base["name2"] = name2
        city = self.db_data.get("COMMUNE", None)
        if city:
            subelement_base["city"] = city
        zipcode = self.db_data.get("CODE", None)
        if zipcode:
            subelement_base["zipcode"] = zipcode
        street, number = self._get_street_number()
        if street:
            subelement_base["street"] = street
        if number:
            subelement_base["number"] = number

        # Filter applicants without name
        if "__children__" in data:
            data["__children__"].append(subelement_base)
        else:
            data["__children__"] = [subelement_base]
        return data

    def requires(self):
        return TransformCadastre(key=self.key)


class DropArchitects(core.DropColumnInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    drop_keys = ["architects"]

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformArchitect(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    log_failure = True

    @property
    def db_data(self):
        return get_db_data(self.key)

    def requires(self):
        return DropArchitects(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/urban/architects/@search"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def transform_data(self, data):
        errors = []
        value = self.db_data["ARCHITECTE"]
        if not value:
            return data, errors
        params = {"SearchableText": f"{value}", "metadata_fields": "UID"}
        r = self.request(parameters=params)
        if r.status_code != 200:
            errors.append(f"Response code is '{r.status_code}', expected 200")
            return data, errors
        result = r.json()
        if result["items_total"] == 0:
            errors.append(f"Aucun résultat pour l'architecte: '{value}'")
            return data, errors
        elif result["items_total"] > 1:
            errors.append(f"Plusieurs résultats pour l'architecte: '{value}'")
            return data, errors
        data["architects"] = [result["items"][0]["UID"]]
        return data, errors


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    drop_keys = [
        "Title",
        'Statistiques INS',
        'geometricians',
        'notaries',
        'referenceDGATLP',
        'review_state',
        'rubrics',
        'type de procédure',
        "wf_state",
        "wf_transition"
    ]

    def requires(self):
        return TransformArchitect(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "gracehollogne"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)

class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "gracehollogne"
    export_filepath = "./result-gracehollogne"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
