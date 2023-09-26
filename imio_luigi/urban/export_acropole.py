# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban.address import find_address_match
from imio_luigi.urban import tools

import json
import logging
import luigi


logger = logging.getLogger("luigi-interface")


class GetFromMySQL(core.GetFromMySQLTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = "urb82003ac"
    tablename = "DOSSIER_VIEW"
    columns = (
        "WRKDOSSIER_ID",
        "DOSSIER_NUMERO",
        "DOSSIER_TDOSSIERID",
        "TDOSSIER_OBJETFR",
        "DOSSIER_DATEDEPART",
        "DOSSIER_DATEDEPOT",
        "DOSSIER_OCTROI",
        "DOSSIER_DATEDELIV",
        "DOSSIER_TYPEIDENT",
        "DOSSIER_REFCOM",
        "DETAILS",
        "CONCAT_PARCELS",
    )

    def run(self):
        for row in self.query():
            data = {k: getattr(row, k) for k in row._fields}
            for column in (
                "DOSSIER_DATEDEPART",
                "DOSSIER_DATEDEPOT",
                "DOSSIER_DATEDELIV",
            ):
                if data[column]:
                    data[column] = data[column].strftime("%Y-%m-%dT%H:%M")
            yield Transform(key=row.WRKDOSSIER_ID, data=data)


class Transform(luigi.Task):
    task_namespace = "acropole"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            f.write(json.dumps(dict(self.data)))
        yield WriteToJSON(key=self.key)


class JoinAddresses(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = "urb82003ac"
    tablename = "ADRESSES_VIEW"
    columns = ["*"]
    destination = "addresses"
    key = luigi.Parameter()

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"


class JoinApplicants(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = "urb82003ac"
    tablename = "DEMANDEURS_VIEW"
    columns = ["*"]
    destination = "applicants"
    key = luigi.Parameter()

    def requires(self):
        return JoinAddresses(key=self.key)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    mapping = {
        "DOSSIER_NUMERO": "reference",
        "DETAILS": "title",
        "TDOSSIER_OBJETFR": "@type",
        "CONCAT_PARCELS": "cadastre",
    }

    def requires(self):
        return JoinApplicants(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    mapping_filepath = "./mapping-type.json"
    mapping_key = "@type"

    def requires(self):
        return Mapping(key=self.key)


class CreateApplicant(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    subelements_source_key = "applicants"
    subelements_destination_key = "__children__"
    mapping_keys = {
        "PRENOM": "name1",
        "NOM": "name2",
        "LOCALITE": "city",
        "ZIP": "zipcode",
        "ADRESSE": "street",
        "TEL": "phone",
        "GSM": "gsm",
    }
    subelement_base = {
        "@type": "Applicant",
        "country": "BE",
    }

    def requires(self):
        return MappingType(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    subelements_source_key = "addresses"
    subelements_destination_key = "workLocations"
    mapping_keys = {
        "ADDRESSE": "street",
        "NUM": "number",
        "ZIP": "zip",
        "LOCALITE": "localite",
    }
    subelement_base = {}

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "acropole"
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


class CadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", " ET ", "et"]

    def requires(self):
        return TransformWorkLocation(key=self.key)


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "acropole"
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
                errors.append(f"Aucun résultat pour la parcelle '{params}'")
                continue
            elif result["items_total"] > 1:
                errors.append(f"Plusieurs résultats pour la parcelle '{params}'")
                continue
            if not "__children__" in data:
                data["__children__"] = []
            new_cadastre = result["items"][0]
            new_cadastre["@type"] = "Parcel"
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
    task_namespace = "acropole"
    key = luigi.Parameter()
    drop_keys = [
        "addresses",
        "applicants",
        "WRKDOSSIER_ID",
    ]

    def requires(self):
        return TransformCadastre(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "acropole"
    export_filepath = "./result"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
