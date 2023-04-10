# -*- coding: utf-8 -*-

from imio_luigi import core, utils

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
        return Mapping(key=self.key)


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


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    mapping_filepath = "./mapping-type.json"
    mapping_key = "@type"

    def requires(self):
        return CreateWorkLocation(key=self.key)


class CadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", " ET "]

    def requires(self):
        return MappingType(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    drop_keys = [
        "addresses",
        "applicants",
        "WRKDOSSIER_ID",
    ]

    def requires(self):
        return CadastreSplit(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "acropole"
    export_filepath = "./result"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
