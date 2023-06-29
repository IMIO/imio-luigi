# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban.address import find_address_match
from imio_luigi.urban import tools

import json
import logging
import luigi
import os
import re


logger = logging.getLogger("luigi-interface")


class GetFromAccess(core.GetFromAccessJSONTask):
    task_namespace = "dison"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    columns = [
        "Numero",
        "LibNat",
        "Rec",
        "Cadastre",
        "C_Adres",
        "C_Loc",
        "C_Code",
        "C_Num",
        "MT",
        "UR_Avis",
        "Avco_Avis",
        "D_Tel",
        "D_GSM",
        "D_Nom",
        "D_Prenom",
        "D_Adres",
        "D_Loc",
        "D_Code",
        "D_Pays",
        "A_Nom",
        "A_Prenom",
        "Section",
        "Recepisse",
        "Autorisa",
        "TutAutorisa",
        "Refus",
        "TutRefus",
        "Memo_Urba",
        "Ordre",
        "Classe",
    ]

    def run(self):
        min_range = None
        max_range = None
        if self.line_range:
            if not re.match("\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            min_range = int(line_range[0])
            max_range = int(line_range[1])
        replacements = {
            "/": "-",
            " ": "_",
        }
        for row in self.query(min_range=min_range, max_range=max_range):
            try:
                if "Numero" not in row:
                    raise ValueError("Missing 'Numero'")
                for k, v in replacements.items():
                    if k in row["Numero"]:
                        row["Numero"] = row["Numero"].replace(k, v)
                if not re.match("^(\d|\w|/|-|\.|\*|_)*$", row["Numero"]):
                    raise ValueError(f"Wrong key {row['Numero']}")
                if row["Rec"] == "/":
                    raise ValueError("Wrong type '/'")
                if "URBAN/" in row["Numero"]:
                    continue
                yield Transform(key=row["Numero"], data=row)
            except Exception as e:
                with self.log_failure_output().open("w") as f:
                    error = {
                        "error": str(e),
                        "data": row,
                    }
                    f.write(json.dumps(error))


class Transform(luigi.Task):
    task_namespace = "dison"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            data = dict(self.data)
            if not "LibNat" in data:
                data["LibNat"] = "Aucun objet"
            if "Memo_Urba" in data:
                data["description"] = {
                    "content-type": "text/html",
                    "data": "<p>{0}</p>\r\n".format(data["Memo_Urba"]),
                }
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    rules_filepath = "./fix-dison.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping = {
        "Numero": "reference",
        "LibNat": "title",
        "Rec": "@type",
        "Cadastre": "cadastre",
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    keys = ("Recepisse", "Avi_Urba", "Autorisa", "Refus", "TutAutorisa", "TutRefus")
    date_format_input = "%m/%d/%y %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    filepath = "./add-data-dison.json"

    def requires(self):
        return ConvertDates(key=self.key)


class MappingCountry(utils.MappingCountryInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping_key = "D_Pays"

    def requires(self):
        return AddExtraData(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping_filepath = "./mapping-type-dison.json"
    mapping_key = "@type"

    def requires(self):
        return MappingCountry(key=self.key)


class MappingAvis(core.MappingValueWithFileInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping_filepath = "./mapping-avis-dison.json"
    mapping_key = "UR_Avis"

    def requires(self):
        return MappingType(self.key)


class AddEvents(core.InMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()

    def requires(self):
        return MappingAvis(key=self.key)

    def transform_data(self, data):
        data = self._create_recepisse(data)
        data = self._create_delivery(data)
        return data

    def _create_recepisse(self, data):
        """Create recepisse event"""
        if "Recepisse" not in data:
            return data
        event_subtype, event_type = self._mapping_recepisse_event(data["@type"])
        event = {
            "@type": event_type,
            "eventDate": data["Recepisse"],
            "urbaneventtypes": event_subtype,
        }
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _create_delivery(self, data):
        columns = ("Autorisa", "TutAutorisa", "Refus", "TutRefus")
        matching_columns = [c for c in columns if c in data]
        if not matching_columns:
            return data
        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        if data.get("Autorisa") or data.get("TutAutorisa"):
            decision = "favorable"
            date = data.get("Autorisa") or data.get("TutAutorisa")
        else:
            decision = "defavorable"
            date = data.get("Refus") or data.get("TutRefus")
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
            "EnvClassThree": ("acceptation-de-la-demande", "UrbanEvent"),
            "PreliminaryNotice": ("passage-college", "UrbanEvent"),
        }
        return data[type]


class AddAttachments(core.InMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    directory = luigi.Parameter()

    def requires(self):
        return AddEvents(key=self.key)

    def get_attachments(self, data):
        path = os.path.join(
            self.directory, self._mapping_folder(data["@type"]), str(data["Ordre"])
        )
        if not os.path.exists(path):
            return []
        return [(f, os.path.join(path, f)) for f in os.listdir(path)]

    def transform_data(self, data):
        for fname, attachment in self.get_attachments(data):
            self._create_attachment(data, fname, attachment)
        return data

    def _create_attachment(self, data, fname, attachment):
        ignored_extensions = (".tmp", ".wbk")
        for ext in ignored_extensions:
            if fname.endswith(ext):
                return
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append({
            "@type": "File",
            "title": fname,
            "file": {
                "data": os.path.abspath(attachment),
                "encoding": "base64",
                "filename": fname,
                "content-type": self._content_type(fname),
            }
        })

    @staticmethod
    def _content_type(fname):
        mapping = {
            ".doc": "application/msword",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".rtf": "application/rtf",
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".csv": "text/csv",
            ".ods": "application/vnd.oasis.opendocument.spreadsheet",
            ".odt": "application/vnd.oasis.opendocument.text",
            ".jpeg": "image/jpeg",
            ".jpg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".bmp": "image/bmp",
            ".txt": "text/plain",
        }
        for key, value in mapping.items():
            if fname.lower().endswith(key):
                return value
        raise ValueError(f"Unknown content type for '{fname}'")

    def _mapping_folder(self, type):
        """Return attachment location based on type"""
        data = {
            "CODT_UrbanCertificateOne": "CU/1/DOSSIERS",
            "CODT_UrbanCertificateTwo": "CU/2/DOSSIERS",
            "UrbanCertificateOne": "CU/1/DOSSIERS",
            "UrbanCertificateTwo": "CU/2/DOSSIERS",
            "PreliminaryNotice": "AVIS PREALABLES/DOSSIERS",
            "CODT_BuildLicence": "URBA/DOSSIERS",
            "BuildLicence": "URBA/DOSSIERS",
            "EnvClassOne": "ENVIRONNEMENT/DOSSIERS",
            "EnvClassTwo": "ENVIRONNEMENT/DOSSIERS",
            "EnvClassThree": "ENVIRONNEMENT/DOSSIERS",
            "ParcelOutLicence": "LOTISSEMENT/DOSSIERS",
            "CODT_ParcelOutLicence": "URBANISATION/DOSSIERS",
            "Declaration": "REGISTRE-PU/DOSSIERS",
            "CODT_UniqueLicence": "UNIQUE/DOSSIERS",
            "UniqueLicence": "UNIQUE/DOSSIERS",
            "MiscDemand": "AUTRE DOSSIER/DOSSIERS",
        }
        return data[type]


class AddTransitions(core.InMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()

    def requires(self):
        return AddAttachments(key=self.key)

    def transform_data(self, data):
        state = None
        if data.get("Autorisa") or data.get("TutAutorisa"):
            state = "accepted"
        elif data.get("Refus") or data.get("TutRefus"):
            state = "refused"
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class CreateApplicant(core.CreateSubElementInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {
        "D_Nom": "name1",
        "D_Prenom": "name2",
        "D_Pays": "country",
        "D_Loc": "city",
        "D_Code": "zipcode",
        "D_Adres": "street",
        "D_Tel": "phone",
        "D_GSM": "gsm",
    }
    subelement_base = {"@type": "Applicant"}

    def transform_data(self, data):
        result = super().transform_data(data)
        # Filter applicants without name
        result["__children__"] = [c for c in result["__children__"] if "name1" in c]
        return result

    def requires(self):
        return AddTransitions(key=self.key)


class WorkLocationSplit(core.StringToListInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    attribute_key = "C_Adres"
    separators = [",", " ET ", " et ", " - "]

    def requires(self):
        return CreateApplicant(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    subelements_source_key = "C_Adres"
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
        for element in data[self.subelements_source_key]:
            new_element = {"street": element}
            mapping = {
                "C_Loc": "city",
                "C_Code": "zip",
                "C_Num": "number",
            }
            for key, destination in mapping.items():
                if key in data:
                    new_element[destination] = data[key]
            data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return WorkLocationSplit(key=self.key)


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "dison"
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

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        for worklocation in data["workLocations"]:
            param_values = [
                str(v)
                for k, v in worklocation.items()
                if v and k in ("street", "locality", "zip")
            ]
            params = {"term": " ".join(param_values)}
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
    task_namespace = "dison"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [tools.CADASTRE_REGEXP]

    def requires(self):
        return TransformWorkLocation(key=self.key)


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CadastreSplit(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/@parcel"

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
            try:
                params = tools.extract_cadastre(cadastre)
            except ValueError:
                errors.append(f"Valeur incorrecte pour la parcelle: {cadastre}")
                continue
            if "Section" in data:
                params["section"] = data["Section"]
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


class TransformArchitect(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "dison"
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
        values = list(filter(None, [data.get("A_Nom"), data.get("A_Prenom")]))
        if not values:
            return data, errors
        search = " ".join(values)
        params = {"SearchableText": f"{search}", "metadata_fields": "UID"}
        r = self.request(parameters=params)
        if r.status_code != 200:
            errors.append(f"Response code is '{r.status_code}', expected 200")
            return data, errors
        result = r.json()
        if result["items_total"] == 0:
            errors.append(f"Aucun résultat pour l'architecte: '{search}'")
            return data, errors
        elif result["items_total"] > 1:
            errors.append(f"Plusieurs résultats pour l'architecte: '{search}'")
            return data, errors
        data["architects"] = [result["items"][0]["UID"]]
        return data, errors


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    drop_keys = [
        "Section",
        "C_Loc",
        "C_Code",
        "C_Adres",
        "C_Num",
        "cadastre",
        "UR_Avis",
        "Avco_Avis",
        "A_Nom",
        "A_Prenom",
        "Autorisa",
        "TutAutorisa",
        "Refus",
        "TutRefus",
        "Memo_Urba",
        "Ordre",
        "Recepisse",
        "Classe",
        "MT",  # temporary
    ]

    def requires(self):
        return TransformArchitect(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class UpdateReference(core.UpdateReferenceInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    rules_filepath = "./reference-dison.json"
    log_failure = True

    def requires(self):
        return ValidateData(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "dison"
    export_filepath = "./result-dison"
    key = luigi.Parameter()

    def requires(self):
        return UpdateReference(key=self.key)
