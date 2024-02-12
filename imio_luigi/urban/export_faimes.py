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
    task_namespace = "faimes"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
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
        "Division",
    ]

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
        # res = utils.get_all_unique_value(self.query(),"Type_U")
        # res = [row.get("Numero", "---No Ref---") for row in self.query()]            
        for row in self.query(min_range=min_range, max_range=max_range):
            with open("./data/faimes/all_ref_to_import.txt", "a") as f:
                f.write(f"{row.get('Numero', '---No Ref---')}\n")
        #     try:
        #         if "Numero" not in row:
        #             msg = "Numero de permis manquant, "
        #             self.on_failure(row, [msg])
        #             self.handle_failure(row, [msg])
        #             row["Numero"] = row["Cle_Urba"]
        #         if not re.match("^(\d|\w|/|-|\.|\*|_| |)*$", row["Numero"]):
        #             raise ValueError(f"Wrong key {row['Numero']}")
        #         if self._complete(row["Numero"]) is True:
        #             continue
        #         if row["Rec"] in ("I", ):  # ignored types
        #             continue
        #         if row["Rec"] == "/":
        #             raise ValueError("Wrong type '/'")
        #         if row["Numero"].startswith("URBAN"):
        #             continue
        #         yield Transform(key=row["Numero"], data=row)
        #     except Exception as e:
        #         with self.log_failure_output().open("w") as f:
        #             error = {
        #                 "error": str(e),
        #                 "data": row,
        #             }
        #             f.write(json.dumps(error))
        #     iteration += 1
        #     if counter and iteration >= counter:
        #         break


class Transform(luigi.Task):
    task_namespace = "faimes"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            data = core.utils.frozendict_to_dict(self.data)
            data["title"] = data["Numero"]
            if "description" not in data:
                data["description"] = {
                    "content-type": "text/html",
                    "data": "",
                }
            if "LibNat" in data:
                data["description"]["data"] += "<p>{0}</p>\r\n".format(data["LibNat"])
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    rules_filepath = "./config/faimes/fix-faimes.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    mapping = {
        "Numero": "reference",
        "Rec": "@type",
        "Cadastre": "cadastre",
        "Memo_Urba": "licenceSubject"
    }

    def requires(self):
        return ValueCleanup(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    keys = ("Recepisse", "Avi_Urba", "Autorisa", "Refus", "TutAutorisa", "TutRefus")
    date_format_input = "%m/%d/%y %H:%M:%S"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def requires(self):
        return Mapping(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    filepath = "./config/faimes/add-data-faimes.json"

    def requires(self):
        return ConvertDates(key=self.key)


class MappingCountry(utils.MappingCountryInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    mapping_key = "D_Pays"

    def requires(self):
        return AddExtraData(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    mapping_filepath = "./config/faimes/mapping-type-faimes.json"
    rubric_mapping = "./config/global/mapping_rubrique_class.json"
    mapping_key = "@type"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    @property
    def get_rubric_mapping(self):
        mapping = json.load(open(self.rubric_mapping, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    def _fix_rubric_key(self, key):
        key = key.strip()
        key = key.replace(" ",".")
        key = key.replace(",",".")
        key = key.replace("O","0")
        key = key.replace("o","0")
        return key

    def _add_class(self, data):
        default_class = "EnvClassTwo"
        lib_nat = data.get("LibNat", None)
        if not lib_nat or lib_nat == "":
            data = self.on_failure(data, [f"Ne trouve pas de code de rubrique"])
            data["@type"] = default_class
            return data
        pattern = r"(?P<rubric>(?:\d\d[\s,.]){2,6}(?:\d{2}|[AB])?)?"
        match = re.match(pattern, data["LibNat"])
        if not match:
            data = self.on_failure(data, [f"Ne trouve pas de code de rubrique"])
            data["@type"] = default_class
            return data
        if match.groupdict()["rubric"] is None:
            data = self.on_failure(data, [f"Ne trouve pas de code de rubrique"])
            if "licenceSubject" not in data:
                data["licenceSubject"] = ""
            data["licenceSubject"] += f" - {data['LibNat']}"
            data["@type"] = default_class
            return data
        output = self.get_rubric_mapping.get(self._fix_rubric_key(match.groupdict()["rubric"]))
        if not output:
            data = self.on_failure(data, [f"Ne trouve pas de rubrique avec le code : {lib_nat}"])
            return data

        data["@type"] = output
        return data

    def transform_data(self, data):
        data = super().transform_data(data)
        if data["@type"] == "EnvClass":
            data = self._add_class(data)
        return data

    def requires(self):
        return MappingCountry(key=self.key)


class AddNISData(ucore.AddNISData):
    task_namespace = "faimes"
    key = luigi.Parameter()

    def requires(self):
        return MappingType(key=self.key)


class MappingAvis(core.MappingValueWithFileInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    mapping_filepath = "./config/faimes/mapping-avis-faimes.json"
    mapping_key = "UR_Avis"

    def requires(self):
        return AddNISData(self.key)


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "faimes"
    key = luigi.Parameter()

    def requires(self):
        return MappingAvis(key=self.key)

    def get_recepisse_check(self, data):
        return "Recepisse" in data

    def get_recepisse_date(self, data):
        return data["Recepisse"]

    def get_delivery_check(self, data):
        columns = ("Autorisa", "TutAutorisa", "Refus", "TutRefus")
        matching_columns = [c for c in columns if c in data]
        return matching_columns

    def get_delivery_date(self, data):
        if data.get("Autorisa") or data.get("TutAutorisa"):
            date = data.get("Autorisa") or data.get("TutAutorisa")
        else:
            date = data.get("Refus") or data.get("TutRefus")
        return date

    def get_delivery_decision(self, data):
        if data.get("Autorisa") or data.get("TutAutorisa"):
            decision = "favorable"
        else:
            decision = "defavorable"
        return decision


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "faimes"
    key = luigi.Parameter()

    def requires(self):
        return AddEvents(key=self.key)


class AddAttachments(core.InMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    directory = luigi.Parameter()

    def requires(self):
        return EventConfigUidResolver(key=self.key)

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
    task_namespace = "faimes"
    key = luigi.Parameter()

    def requires(self):
        return EventConfigUidResolver(key=self.key)

    def transform_data(self, data):
        state = None
        if data.get("Autorisa") or data.get("TutAutorisa"):
            state = "accepted"
        elif data.get("Refus") or data.get("TutRefus"):
            state = "refused"
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class UrbanTransitionMapping(ucore.UrbanTransitionMapping):
    task_namespace = "faimes"
    key = luigi.Parameter()

    def requires(self):
        return AddTransitions(key=self.key)


class CreateApplicant(core.CreateSubElementInMemoryTask):
    task_namespace = "faimes"
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
        result["__children__"] = [c for c in result["__children__"] if c["@type"] != "Applicant" or "name1" in c]
        return result

    def requires(self):
        return UrbanTransitionMapping(key=self.key)


class WorkLocationSplit(core.StringToListInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    attribute_key = "C_Adres"
    separators = [",", " ET ", " et ", " - "]

    def requires(self):
        return CreateApplicant(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    subelements_source_key = "C_Adres"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = False

    def _handle_street(self,street):
        pattern = r"(?P<street>.*|)\((?P<locality>.*)\)"
        match = re.match(pattern,street)
        if not match:
            return {"street": street}
        return match.groupdict()

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
            new_element = self._handle_street(element)
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


class TransformWorkLocation(ucore.TransformWorkLocation):
    task_namespace = "faimes"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CreateWorkLocation(key=self.key)

    def _generate_term(self, worklocation, data):
        param_values = [
            str(v)
            for k, v in worklocation.items()
            if v and k in ("street", "locality", "zip")
        ]
        return " ".join(param_values), None


class CadastreSplit(core.StringToListRegexpInMemoryTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [tools.CADASTRE_REGEXP]

    def requires(self):
        return TransformWorkLocation(key=self.key)


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "faimes"
    key = luigi.Parameter()
    log_failure = True
    mapping_division_dict = {
        1: "64016",
        2: "61009",
        3: "61037",
        4: "64070",
        5: "61002",
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
        if "Section" in data:
            params["section"] = data["Section"]
        if "Division" in data:
            params["division"] = self._mapping_division(data["Division"])

        return self._fix_lower_chars(params), None


class TransformArchitect(ucore.TransformContact):
    task_namespace = "faimes"
    key = luigi.Parameter()
    log_failure = True
    contact_type = "architects"
    data_key = "architects"

    def requires(self):
        return TransformCadastre(key=self.key)

    def _fix_term(self, term):
        term = term.replace("(", " ")
        term = term.replace(")", " ")
        term = term.replace("  ", " ")
        return term

    def _generate_contact_name(self, data):
        values = list(filter(None, [data.get("A_Nom"), data.get("A_Prenom")]))
        if not values:
            return None, None
        return self._fix_term(" ".join(values)), None


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "faimes"
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
        'Division',
        'LibNat'
    ]

    def requires(self):
        return TransformArchitect(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "faimes"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "faimes"
    export_filepath = "./result-faimes"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
