# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban.address import find_address_match
from imio_luigi.urban import tools

import json
import logging
import luigi
import os
import re
import copy

logger = logging.getLogger("luigi-interface")


class GetFromXML(core.GetFromListXMLFile):
    task_namespace = "ecaussinnes"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    fail_on_error = True
    files= [
        "liste804.utf8.xml",
        "liste809.utf8.xml",
        "liste811.utf8.xml",
        "liste812.utf8.xml",
        "liste813.utf8.xml",
    ]
    parameter = {
        "liste804.utf8.xml": [{"type": "EnvClassThree"}],
        "liste809.utf8.xml": [{"type": "NotaryLetter"}],
        "liste811.utf8.xml": [
            {"pattern": r"PE", "type": "EnvClassTwo"},
            {"pattern": r"PU", "type": "UniqueLicence"},
            {"pattern": r"[Aa]rt65", "type": "EnvClassTwo"},
            {"type": "EnvClassTwo"},
        ],
        "liste812.utf8.xml": [
            {"pattern": r"PE", "type": "EnvClassTwo"},
            {"pattern": r"PU", "type": "UniqueLicence"},
            {"pattern": r"[Aa]rt65", "type": "EnvClassTwo"},
            {"pattern": r"CL3", "type": "EnvClassThree"},
            {"type": "EnvClassTwo"},
        ],
        "liste813.utf8.xml": [
            {"pattern": r"PE", "type": "EnvClassTwo"},
            {"pattern": r"PU", "type": "UniqueLicence"},
            {"pattern": r"[Aa]rt65", "type": "EnvClassTwo"},
            {"type": "EnvClassTwo"},
        ],
    }

    def _complete(self, key):
        """Method to speed up process that verify if output exist or not"""
        task = WriteToJSON(key=key)
        return task.complete()

    def apply_type(self, key, file):
        licence_type = self.parameter[file]
        for pattern in licence_type:
            if "pattern" not in pattern:
                return pattern["type"]
            if re.match(f'.*{pattern["pattern"]}.*', key):
                return pattern["type"]
        return licence_type[-1]["type"]

    def get_title(self, data):
        max_width_text = 150
        title_obj = data["wrkdossier_obj"]
        if len(title_obj) > max_width_text:
            title_obj = f"{title_obj[:max_width_text]} ..."
        return f"{data['reference']} - {title_obj}"

    def fix_ref(self, ref):
        ref = ref.replace("(", "-")
        ref = ref.replace(")","")
        ref = ref.replace(" ", "_") 
        return ref

    def transform_data(self, data, file):
        ref = data.get("RefCom", data.get("wrkdossier_num", None))
        if not ref:
            with self.log_failure_output().open("w") as f:
                error = {
                    "error": str('Missing Ref'),
                    "data": data,
                }
                f.write(json.dumps(error))
            raise ValueError("Missing ref")
        data["reference"] = self.fix_ref(ref)
        data["title"] = self.get_title(data)
        data["@type"] = self.apply_type(ref, file)
        return data

    def run(self):
        min_range = None
        max_range = None
        counter = None
        if self.line_range and self.line_range != 'None':
            if not re.match(r"\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            min_range = int(line_range[0])
            max_range = int(line_range[1])
        if self.counter and self.counter != 'None':
            counter = int(self.counter)
        iteration = 0
        res = utils.get_all_unique_value_with_callback(self.query(), fetch_num)
        for row in self.query(min_range=min_range, max_range=max_range):
            try:
                yield Transform(key=row["reference"], data=row)
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
    task_namespace = "ecaussinnes"
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
        
    def run(self):
        try:
            with self.output().open("w") as f:
                data = dict(self.data)
                f.write(json.dumps(data))
            yield WriteToJSON(key=self.key)
        except Exception as e:
            self._handle_exception(data, e, f)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    rules_filepath = "./config/ecaussinnes/fix-ecaussinnes.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class AddNISData(tools.AddNISData):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()

    def requires(self):
        return ValueCleanup(key=self.key)

class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    mapping = {
        "wrkdossier_obj": "licenceSubject",
    }

    def requires(self):
        return AddNISData(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    keys = ("DateInsert", "DateDeliv")
    date_format_input = "%d/%m/%Y"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    filepath = "./config/ecaussinnes/add-data-ecaussinnes.json"

    def requires(self):
        return ConvertDates(key=self.key)


class AddEvents(core.InMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()

    def requires(self):
        return AddExtraData(key=self.key)

    def transform_data(self, data):
        data = self._create_recepisse(data)
        data = self._create_delivery(data)
        return data

    def _create_recepisse(self, data):
        """Create recepisse event"""
        if "DateInsert" not in data:
            return data
        event_subtype, event_type = self._mapping_recepisse_event(data["@type"])
        event = {
            "@type": event_type,
            "eventDate": data["DateInsert"],
            "urbaneventtypes": event_subtype,
        }
        date = data.get("DateInsert", None)
        if date or date :
            event["decisionDate"] = date
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _create_delivery(self, data):
        if "DateDeliv" not in data:
            return data
        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        if data.get("etat") in [
            'Dossier recevable',
            'Déc. recevable avec cond. compl.',
            'Déc. recevable sans cond. compl.',
            'Octroyé sur recours',
            'Permis octroyé par le FT',
            'Permis octroyé',
            'Révision octroyée par le collège',
            'Révision octroyée par le FT'
        ]:
            decision = "favorable"
        elif data.get("etat") in ['Demande irrecevable', 'Dossier irrecevable','Permis refusé', 'Refusé sur recours', 'Révision refusée par le FT']:
            decision = "defavorable"
        else: 
            return data
        event = {
            "@type": event_type,
            "decision": decision,
            "urbaneventtypes": event_subtype,
        }
        date = data.get("DateDeliv", None)
        if date or date :
            event["decisionDate"] = date
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _mapping_recepisse_event(self, type):
        data = {
            "BuildLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_BuildLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
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
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    
    def requires(self):
        return AddEvents(key=self.key)


class AddTransitions(core.InMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()

    mapping = {
        'Annulé / Abandonné': "retired",
        'Demande irrecevable': "inacceptable",
        'Dossier en cours (étape non clôturée)': "deposit",
        'Dossier en cours': "deposit",
        'Dossier irrecevable': "inacceptable",
        'Dossier recevable': "accepted",
        'Déc. recevable avec cond. compl.': "accepted",
        'Déc. recevable sans cond. compl.': "accepted",
        'Octroyé sur recours': "accepted",
        'Permis octroyé par le FT': "accepted",
        'Permis octroyé': "accepted",
        'Permis refusé': "refused",
        'Recours en cours': "complete",
        'Refusé sur recours': "refused",
        'Révision octroyée par le collège': "accepted",
        'Révision octroyée par le FT': "accepted",
        'Révision refusée par le FT': "refused",
    }

    def requires(self):
        return EventConfigUidResolver(key=self.key)

    def transform_data(self, data):
        state = self.mapping[data.get("etat")]
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class CreateApplicant(core.CreateSubElementInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {}
    subelement_base = {"@type": "Applicant"}

    def _merge_all_name(self, value):
        if len(value) <= 2:
            return value[0]
        copy_value = copy.deepcopy(value)
        copy_value.pop()
        copy_value.pop()

        return " - ".join(copy_value)

    def _extract_zip_code(self, data):
        match = re.match("(\d{0,4})\s*([a-zA-Z-\s']*)",data)
        if not match:
            self.subelement_base["city"] = data
            return
        if match.groups()[0] != '':
            self.subelement_base["city"] = match.groups()[0]
        if match.groups()[1] != '':
            self.subelement_base["zipcode"] = match.groups()[1]

    def _extract_street_number(self, data):
        match = re.match("([a-zA-Z-\s']*)[,\s+]*(\d{0,4}[a-zA-A]*)",data)
        if not match:
            self.subelement_base["street"] = data
            return
        if match.groups()[0] != '':
            self.subelement_base["street"] = match.groups()[0]
        if match.groups()[1] != '':
            self.subelement_base["number"] = match.groups()[1]
        

    def transform_data(self, data):
        applicant = data.get("coordonnees_demandeur", None)
        if not applicant:
            return data

        applicant_split = [item.strip() for item in applicant.split(" - ")]
        if applicant_split[-1] == "d'Enghien":
            applicant_split.pop()
            applicant_split[-1] = f"{applicant_split[-1]} - d'Enghien"
        name1 = self._merge_all_name(applicant_split)
        self.subelement_base["name1"] = name1
        data['title'] += f" - {name1}"
        if len(applicant_split) >= 3:
            if not applicant_split[-2] == "":
                self._extract_street_number(applicant_split[-2])
            if not applicant_split[-1] == "" or not applicant_split[-1] == "0":
                self._extract_zip_code(applicant_split[-1])

        # Filter applicants without name
        if "__children__" in data:
            data["__children__"].append(self.subelement_base)
        else:
            data["__children__"] = [self.subelement_base]
        return data

    def requires(self):
        return AddTransitions(key=self.key)


class WorkLocationStreetSplit(core.StringToListInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    attribute_key = "sitbien"
    separators = [","]
    city = ""
    ignore_missing = False

    def _fix_item(self, item):
        item = self._extract_house_number(item)
        item["street"] = str(item["street"]).strip()
        if "number" in item:
            item["number"] = str(item["number"]).strip()
        return item

    def _extract_house_number(self, value):
        pattern = r"(.*?)\s\+?(\d+[A-Za-z+\s\d/]*)"
        result = [item for item in re.split(pattern, value) if item != ""]
        output = {"street": result[0]}
        if len(result) >= 2:
            output["number"] = result[1]
        return output

    def _remove_city(self, value):
        pattern = r"(.*?)\s*-\s*((?:\d{4}\s[A-Za-z\-\s']+))"
        street_city_sep = [item for item in re.split(pattern, value) if item != ""]
        streets = street_city_sep[0]
        if len(street_city_sep) >= 2:
            self.city = str(street_city_sep[1]).strip()
        return streets

    def transform_data(self, data):
        if not data[self.attribute_key]:
            return data
        data[self.attribute_key] = self._remove_city(data[self.attribute_key])
        data = super(WorkLocationStreetSplit, self).transform_data(data)
        data["locality"] = self.city
        return data

    def requires(self):
        return CreateApplicant(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    subelements_source_key = "sitbien"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = True

    def transform_data(self, data):
        if not data["sitbien"]:
            data[self.subelements_destination_key] = []
            return data
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
        if self.subelements_source_key not in data:
            if self.ignore_missing is False:
                raise KeyError(f"Missing key {self.subelements_source_key}")
            return data
        for element in data[self.subelements_source_key]:
            element["locality"] = data["locality"]
            data[self.subelements_destination_key].append(element)
        return data

    def requires(self):
        return WorkLocationStreetSplit(key=self.key)


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CreateWorkLocation(key=self.key)

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

    def _lieu_dit_cleanup(self, value):
        if value.startswith("lieu-dit"):
            value = value.replace('"', '')
        return value

    def _prepare_term(self, worklocation):
        return {
            "term": f"{self._lieu_dit_cleanup(worklocation['street'])} ({worklocation['locality']})",
            "include_disable": "true"
        }

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        for worklocation in data["workLocations"]:
            params = self._prepare_term(worklocation)
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
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

            new_work_locations.append(
                {
                    "street": match["uid"],
                    "number": worklocation.get("number", ""),
                }
            )
        data["workLocations"] = new_work_locations
        return data, errors


class CadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [","]

    def _fix_item(self, item):
        item.strip()
        return item

    def requires(self):
        return TransformWorkLocation(key=self.key)


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CadastreSplit(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/@parcel"

    def _mapping_division(self, data):
        mapping = {
            "01": "55008",
            "02": "55009",
            "03": "55024",
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

    def get_bis(self, radical):
        split_rad = radical.split("/")
        bis = ""
        if len(split_rad) > 1:
            bis = split_rad[1]
            radical = split_rad[0]
        return radical, bis

    def transform_data(self, data):
        if "cadastre" not in data or not data['cadastre']:
            return data
        errors = []
        for cadastre in data["cadastre"]:
            if cadastre == "Non cadastré":
                continue
            pattern = r"(?P<division>\d{1,4})\s*(?P<section>[a-zA-Z])\s*(?P<radical>\d{0,4})\/?(?P<bis>\d{0,2})\s*(?P<exposant>[a-zA-Z]?)\s*(?P<puissance>\d{0,2})"
            cadastre_split = re.match(pattern,cadastre.strip())
            if not cadastre_split:
                errors.append(f"Impossible de reconnaitre la parcelle '{cadastre}'")
                continue
            params = cadastre_split.groupdict()
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
            if not "__children__" in data:
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


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    drop_keys = [
        "RefCom",
        "DateInsert",
        "sitbien",
        "wrkdossier_obj",
        "coordonnees_demandeur",
        "etat",
        "DateDeliv",
        'cadastre',
        'key',
        'locality',
        'DatPasCE',
        'RefPasCE',
        'natdem',
        'wrkdossier_num'
    ]

    def requires(self):
        return TransformCadastre(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "ecaussinnes"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "ecaussinnes"
    export_filepath = "./result-ecaussinnes"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
