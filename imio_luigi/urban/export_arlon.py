# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.core.utils import _cache
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools
from imio_luigi.urban.address import find_address_match

import json
import logging
import luigi
import os
import re


logger = logging.getLogger("luigi-interface")


class GetFromAccess(core.GetFromAccessJSONTask):
    task_namespace = "arlon"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    columns = [
        "auteur_de_projet",
        "clé",
        "année",
        "appartements",
        "division_cadastrale",
        "référence_DGATLP",
        "irrecevabilité",
        "référence_communale",
        "rue-place",
        "entrée",
        "objet",
        "refus",
        "remarques",
        "n°_cadastral",
        "section_cadastrale",
        "mandataire",
        "octroi",
        "logements régularisés",
        "début_travaux",
        "maisons",
        "type_dossier",
        "localité",
        "référence_communale_Old",
        "n°_dossier",
        "demandeur",
        "lotissement",
        "Charges d'urbanisme",
        "n°",
        "lot"
    ]

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
            if self._complete(str(row["clé"])) is True:
                continue
            if len(row) <= 2:
                continue
            try:
                yield Transform(key=row["clé"], data=row)
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
    task_namespace = "arlon"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def make_title(self, data):
        title = ""
        ref = data.get("référence_communale", None)
        if ref:
            title = ref
        object = data.get("objet", None)
        if object:
            title += " - " + object
        data["title"] = title
        return data

    def run(self):
        with self.output().open("w") as f:
            data = dict(self.data)
            if "remarques" in data:
                data["description"] = {
                    "content-type": "text/html",
                    "data": "<p>{0}</p>\r\n".format(data["remarques"]),
                }
            data = self.make_title(data)
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    rules_filepath = "./config/arlon/fix-arlon.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)



class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    mapping = {
        "référence_communale": "reference",
        "objet": "licenceSubject",
        "type_dossier": "@type",
        "n°_cadastral": "cadastre",
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    keys = ("entrée", "octroi", "refus", "début_travaux", "irrecevabilité")
    date_format_input = "%m/%d/%y %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    filepath = "./config/arlon/add-data-arlon.json"

    def requires(self):
        return ConvertDates(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    log_failure = True
    mapping_filepath = "./config/arlon/mapping-type-arlon.json"
    mapping_key = "@type"
    codt_trigger = [
        "UrbanCertificateOne",
        "UrbanCertificateTwo",
        "Article127",
        "BuildLicence",
        "ParcelOutLicence",
        "NotaryLetter",
        "UniqueLicence",
        "IntegratedLicence"
    ]
    codt_start_date = datetime(2017, 6, 1)
    codt_start_year = 2017
    
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

    def _certificate_one_two(self, data):
        if self.mapping_key not in data or data[self.mapping_key] == "UrbanCertificate":
            if "title" not in data:
                return data
            suffix = "One"
            match = re.match(r"^CU([12])", data["title"])
            if match and match.groups()[0] == '2':
                suffix = "Two"
            data[self.mapping_key] = f"{data[self.mapping_key]}{suffix}"
        
        return data

    def get_secondary_date(self, data):
        date_keys = ["octroi", "refus", "début_travaux", "irrecevabilité"]
        for key in date_keys:
            if key not in data:
                continue
            date = datetime.fromisoformat(data[key])
            if date < self.codt_start_date:
                return True
        return False

    def _cwatup_codt(self, data):
        if data["@type"] not in self.codt_trigger:
            return data
        year = None
        date = data.get("entrée", None)
        if date:
            date = datetime.fromisoformat(date)
        else:
            year = data.get("année", None)

        if not (date or year != self.codt_start_year):
            if not self.get_secondary_date(data):
                raise KeyError("Manque une date pour déterminer si c'est un permis CODT")

        if (date and date > self.codt_start_date) or (year and year > self.codt_start_year):
            data["@type"] = f"CODT_{data['@type']}"

        return data
    
    def extract_type(self, data):
        type = data.get("@type", None)
        
        if type:
            return data
        ref = data.get("reference")
        
        if not ref:
            raise ValueError("Missing type and reference")

        return data
            

    def transform_data(self, data):
        data = self.extract_type(data)
        data = super(MappingType, self).transform_data(data)
        data = self._certificate_one_two(data)
        data = self._cwatup_codt(data)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try :
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)

    def requires(self):
        return AddExtraData(key=self.key)


class AddNISData(ucore.AddNISData):
    task_namespace = "arlon"
    key = luigi.Parameter()

    def requires(self):
        return MappingType(key=self.key)


class AddEvents(core.InMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()

    def requires(self):
        return AddNISData(key=self.key)

    def transform_data(self, data):
        data = self._create_recepisse(data)
        data = self._create_delivery(data)
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
        columns = ("octroi", "irrecevabilité", "refus")
        matching_columns = [c for c in columns if c in data]
        if not matching_columns:
            return data
        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        if data.get("octroi"):
            decision = "favorable"
            date = data.get("octroi")
        else:
            decision = "defavorable"
            date = data.get("irrecevabilité") or data.get("refus")
        event = {
            "@type": event_type,
            "decision": decision,
            "decisionDate": date,
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
            "Article127": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_Article127": ("reception-demande-dgo4-codt", "UrbanEvent"),
            "IntegratedLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_IntegratedLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "Declaration": ("depot-de-la-demande", "UrbanEvent"),
            "UrbanCertificateOne": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateOne": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UrbanCertificateTwo": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateTwo": ("depot-demande", "UrbanEvent"),
            "PreliminaryNotice": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassOne": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassTwo": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassThree": ("depot-de-la-demande", "UrbanEvent"),
            "ParcelOutLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "MiscDemand": ("depot-de-la-demande", "UrbanEvent"),
            "PatrimonyCertificate": ("depot-de-la-demande-codt", "UrbanEvent"),
            "Ticket": ("depot-de-la-demande-codt", "UrbanEvent"),
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
            "CODT_Article127": ("transmis-decision-fd-codt", "UrbanEvent"),
            "ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"),
            "IntegratedLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_IntegratedLicence": ("delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"),
            "Declaration": ("deliberation-college", "UrbanEvent"),
            "UrbanCertificateOne": ("octroi-cu1", "UrbanEvent"),
            "UrbanCertificateTwo": ("octroi-cu2", "UrbanEvent"),
            "UniqueLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_UniqueLicence": ("delivrance-permis", "UrbanEvent"),
            "MiscDemand": ("deliberation-college", "UrbanEvent"),
            "EnvClassOne": ("decision", "UrbanEvent"),
            "EnvClassTwo": ("decision", "UrbanEvent"),
            "EnvClassThree": ("acceptation-de-la-demande", "UrbanEvent"),
            "PreliminaryNotice": ("passage-college", "UrbanEvent"),
            "PatrimonyCertificate": ("decision", "UrbanEvent"),
            "Ticket": ("decision", "UrbanEvent"),
        }
        return data[type]


class AddOtherEvent(ucore.AddEvents):
    task_namespace = "arlon"
    key = luigi.Parameter()
    event_config = {
        "travaux": {
            "check_key": "début_travaux",
            "date_mapping": {"eventDate": "début_travaux"},
            "mapping": {
                "BuildLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_BuildLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "Article127": {"urban_type": "debut-des-travaux", "date": ["eventDate"]}
            }
        }
    }
    
    def requires(self):
        return AddEvents(key=self.key)

class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "arlon"
    key = luigi.Parameter()
    
    def requires(self):
        return AddOtherEvent(key=self.key)
    
    
class AddTransitions(core.InMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()

    def requires(self):
        return EventConfigUidResolver(key=self.key)

    def transform_data(self, data):
        state = "deposit"
        if data.get("octroi"):
            state = "accepted"
        elif data.get("refus"):
            state = "refused"
        elif data.get("irrecevabilité"):
            state = "inacceptable"
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class UrbanTransitionMapping(ucore.UrbanTransitionMapping):
    task_namespace = "arlon"
    key = luigi.Parameter()
    
    def requires(self):
        return AddTransitions(key=self.key)


class WorkLocationStreetSplit(core.StringToListInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    attribute_key = "rue-place"
    separators = [" - "]

    def _fix_item(self, item):
        return item.strip()

    def requires(self):
        return UrbanTransitionMapping(key=self.key)


class WorkLocationNumberSplit(core.StringToListInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    attribute_key = "n°"
    separators = ["-"]

    def _fix_item(self, item):
        return item.strip()

    def requires(self):
        return WorkLocationStreetSplit(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    subelements_source_key = "rue-place"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = True

    def _prepeare_street(self, street):
        street = re.sub(r"\([A-Z][a-z]*\)$", "", street)
        street = street.split(' / ')
        street.reverse()
        return " ".join(street) 

    def transform_data(self, data):
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
        if self.subelements_source_key not in data:
            if self.ignore_missing is False:
                raise KeyError(f"Missing key {self.subelements_source_key}")
            return data
        for index, element in enumerate(data[self.subelements_source_key]):
            new_element = {"street": self._prepeare_street(element)}
            if "localité" in data:
                new_element["locality"] = data["localité"]
            if "n°" in data and len(data["n°"]) >= index + 1:
                new_element["number"] = data["n°"][index].strip()

            data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return WorkLocationNumberSplit(key=self.key)


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "arlon"
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

    def _prepare_term(self, worklocation):
        term = ""
        if "locality" in worklocation:
            term += worklocation['locality']
        if "street" in worklocation:
            term += f", {worklocation['street']}"
        return {"term": term}

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


class CadastreSplit(core.StringToListRegexpInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = ["\d{1,4} {0,1}\w{1}\d{0,2}"]

    def requires(self):
        return TransformWorkLocation(key=self.key)


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CadastreSplit(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/@parcel"

    def _mapping_division(self, data):
        mapping = {
            "1": "81001",
            "3": "81005",
            "4": "81006",
            "5": "81007",
            "6": "81011",
            "7": "81021",
            "8": "81408",
        }
        if 'division_cadastrale' not in data or data['division_cadastrale'] not in mapping:
            return "99999"
        return mapping[data['division_cadastrale']]

    def _fix_cadastre(self, cadastre):
        cadastre = cadastre.replace("à", "a")

        return cadastre

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

    def transform_data(self, data):
        errors = []
        for cadastre in data["cadastre"]:
            cadastre = self._fix_cadastre(cadastre)
            try:
                params = tools.extract_cadastre(cadastre)
            except ValueError:
                errors.append(f"Valeur incorrecte pour la parcelle: {cadastre}")
                continue
            params["section"] = data['section_cadastrale'].split('/')[0].strip()
            params["division"] = self._mapping_division(data)
            params["browse_old_parcels"] = True
            r = self.request(parameters=params)
            if r.status_code != 200:
                __import__('pdb').set_trace()
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


class TransformArchitect(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return TransformCadastre(key=self.key)

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
        value = data.get("auteur_de_projet", None)
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


class CreateApplicant(core.CreateSubElementInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    log_failure = True
    subelement_container_key = "__children__"
    mapping_keys = {}
    subelement_base = {"@type": "Applicant"}
    demandeur_path = "./data/arlon/json/DEMANDEURS.json"
    mapping = {
        'localité': "city",
        'n°': "number",
        'nom': "name1",
        'GSM': "gsm",
        'code_postal': "zipcode",
        'adresse': "street",
        'fax': "fax",
        'société': "society",
        'téléphone': "phone",
        'prénom': "name2",
        'pays': "country",
        'Email': "email",
        "civilité": "personTitle",
        "statut": "legalForm"
    }
    mapping_civilte = {
        'M.': "mister",
        'Mme': "madam",
        'M. & Mme': "madam_and_mister",
        'M. / Mme': "madam_and_mister",
        'M. et Mlle': "mademoiselle-et-monsieur",
        'Mlle': "miss"
    }

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

    @property
    @_cache(ignore_args=True)
    def get_table(self):
        file_content = open(self.demandeur_path, "r")
        return [
            json.loads(line)
            for line in file_content.readlines()
        ]

    def mapping_demandeur_keys(self, obj):
        new_obj = {}
        for original, destination in self.mapping.items():
            if original in obj:
                new_obj[destination] = obj[original]

        if "personTitle" in new_obj:
            title = self.mapping_civilte.get(new_obj["personTitle"], None)
            if title:
                new_obj["personTitle"] = title
            else:
                del new_obj["personTitle"]
        return new_obj

    def get_demandeur_data(self, demandeur):
        result = [
            item
            for item in self.get_table
            if item.get("demandeur", "") == demandeur
        ]
        if len(result) == 0:
            result = [
                item
                for item in self.get_table
                if re.match(demandeur, item.get("demandeur", ""))
            ]

        if len(result) == 1:
            self.subelement_base = {
                **self.subelement_base,
                **self.mapping_demandeur_keys(result[0])
            }
        if (len(result) > 1 or len(result) == 0):
            self.subelement_base = {
                **self.subelement_base,
                "name1": demandeur
            }

    def transform_data(self, data):
        applicant = data.get("demandeur", None)
        if not applicant or applicant == "?":
            return data

        self.subelement_base["name1"] = applicant
        data['title'] += f" - {applicant}"

        self.get_demandeur_data(applicant)

        if "legalForm" in self.subelement_base:
            self.subelement_base["@type"] == "Corporation"

        if "country" in self.subelement_base:
            maping_country = {"BELGIQUE": "belgium", "G.D. LUXEMBOURG": "luxembourg"}
            self.subelement_base["country"] = maping_country[self.subelement_base["country"]]

        # Filter applicants without name
        if "__children__" in data:
            data["__children__"].append(self.subelement_base)
        else:
            data["__children__"] = [self.subelement_base]
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try :
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)

    def requires(self):
        return TransformArchitect(key=self.key)


class AddValuesInDescription(ucore.AddValuesInDescription):
    task_namespace = "arlon"
    key = luigi.Parameter()
    title = "Info complémentaire"
    key_to_description = [
        "lotissement",
        "lot",
        "référence_DGATLP",
        "mandataire",
        "maisons",
        "appartements",
        "logements régularisés",
        "Charges d'urbanisme",
        "référence_communale_Old",
    ]
    key_dict = {
        "référence_communale_Old": "Ancien référence communale",
        "référence_DGATLP": "Référence DGATLP",
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
        return CreateApplicant(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    drop_keys = [
        "année",
        "auteur_de_projet",
        "clé",
        "demandeur",
        "division_cadastrale",
        "entrée",
        "localité",
        "n°_cadastral",
        "objet",
        "remarques",
        "rue-place",
        "référence_communale",
        "section_cadastrale",
        "type_dossier",
        "n°",
        "cadastre",
        "octroi",
        "refus",
        "début_travaux",
        "irrecevabilité",
        "lot",
        "lotissement",
        "n°_dossier",
        "référence_DGATLP",
        "mandataire",
        "appartements",
        "maisons",
        "logements régularisés",
        "référence_communale_Old",
        "Charges d'urbanisme"
    ]

    def requires(self):
        return AddValuesInDescription(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "arlon"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "arlon"
    export_filepath = "./result-arlon"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
