# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools
from imio_luigi.urban.address import find_address_match

import json
import logging
import luigi
import re


logger = logging.getLogger("luigi-interface")


class GetFromMySQL(core.GetFromMySQLTask):
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    orga = luigi.Parameter()
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
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

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def run(self):
        limit = None
        offset = None
        if self.counter:
            limit = int(self.counter)
            offset = None
        if self.line_range:
            if not re.match(r"\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            limit = int(line_range[0])
            offset = int(line_range[1]) - int(line_range[0])
        for row in self.query(limit=limit, offset=offset):
            data = {k: getattr(row, k) for k in row._fields}
            for column in (
                "DOSSIER_DATEDEPART",
                "DOSSIER_DATEDEPOT",
                "DOSSIER_DATEDELIV",
            ):
                if data[column]:
                    data[column] = data[column].strftime("%Y-%m-%dT%H:%M")
            yield Transform(key=row.WRKDOSSIER_ID, data=data, orga=self.orga)


class Transform(luigi.Task):
    task_namespace = "acropole"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            f.write(json.dumps(dict(self.data)))
        yield WriteToJSON(key=self.key, orga=self.orga)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    filepath = luigi.OptionalParameter(default=None)

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def transform_data(self, data):
        if not self.filepath:
            return data
        return super().transform_data(data)

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class JoinAddresses(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "ADRESSES_VIEW"
    columns = ["*"]
    destination = "addresses"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return AddExtraData(key=self.key, orga=self.orga)

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
    dbname = luigi.Parameter()
    tablename = "DEMANDEURS_VIEW"
    columns = ["*"]
    destination = "applicants"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinAddresses(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"


class JoinArchitect(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "ARCHI_VIEW"
    columns = ["*"]
    destination = "architects"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinApplicants(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"


def handle_date(data, column_date=[]):
    for column in column_date:
        if data[column]:
            data[column] = data[column].strftime("%Y-%m-%dT%H:%M")
    return data


class JoinEvents(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "EVENTS_VIEW"
    columns = ["*"]
    destination = "all_events"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinArchitect(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"

    def hook_before_serialization(self, values):
        return [handle_date(value, ["ETAPE_DATEDEPART","ETAPE_INS_DATE"]) for value in values]


class JoinEvents2(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "EVENTS_VIEW_2"
    columns = ["*"]
    destination = "all_events"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinEvents(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"

    def hook_before_serialization(self, values):
        return [handle_date(value, ["ETAPE_INS_DATE"]) for value in values]


class JoinOpinions(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "AVIS_VIEW"
    columns = ["*"]
    destination = "all_opinions"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinEvents2(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"

    def hook_before_serialization(self, values):
        return [handle_date(value, ["AVIS_DATE"]) for value in values]


class JoinDocumentsA(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "DOCUMENT_VIEW"
    columns = ["*"]
    destination = "documents"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinOpinions(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"

    def hook_before_serialization(self, values):
        return [handle_date(value, ["create_date"]) for value in values]


class JoinDocumentsB(JoinDocumentsA):
    task_namespace = "acropole"
    tablename = "DOCUMENT_VIEW_2"
    dbname = luigi.Parameter()
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def requires(self):
        return JoinDocumentsA(key=self.key, orga=self.orga)

    def hook_before_serialization(self, values):
        return [handle_date(value, ["delivery_date", "create_date"]) for value in values]


class JoinDocumentsC(core.JoinFromMySQLInMemoryTask):
    task_namespace = "acropole"
    login = "root"
    password = "password"
    host = "localhost"
    port = 3306
    dbname = luigi.Parameter()
    tablename = "DOCUMENT_EVENT_VIEW_1"
    columns = ["*"]
    destination = "documents_event"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    destination_type = "dict"

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinDocumentsB(key=self.key, orga=self.orga)

    def sql_condition(self):
        with self.input().open("r") as f:
            data = json.load(f)
            return f"WRKDOSSIER_ID = {data['WRKDOSSIER_ID']}"

    def hook_before_serialization(self, values):
        values = [handle_date(value, ["date_document", "date_archive", "date_creation"]) for value in values]
        values = {value["event_id"]: value for value in values}
        return values


class JoinDocumentsD(JoinDocumentsC):
    task_namespace = "acropole"
    tablename = "DOCUMENT_EVENT_VIEW_2"
    dbname = luigi.Parameter()
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def requires(self):
        return JoinDocumentsC(key=self.key, orga=self.orga)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    mapping = {
        "DOSSIER_NUMERO": "reference",
        "DETAILS": "licenceSubject",
        "DOSSIER_TDOSSIERID": "@type",
        "CONCAT_PARCELS": "cadastre",
        "DOSSIER_OCTROI": "wf_transitions",
    }

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return JoinDocumentsD(key=self.key, orga=self.orga)


class MappingType(ucore.UrbanTypeMapping):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    mapping_filepath = "./config/acropole/mapping-type-acropole.json"
    mapping_key = "@type"

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return Mapping(key=self.key, orga=self.orga)


class AddNISData(ucore.AddNISData):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return MappingType(key=self.key, orga=self.orga)


class AddTransitions(core.MappingValueWithFileInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    mapping_filepath = "./config/acropole/mapping-transition-acrople.json"
    mapping_key = "wf_transitions"

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    @property
    @core.utils._cache(ignore_args=True)
    def mapping(self):
        mapping = json.load(open(self.mapping_filepath, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    def requires(self):
        return AddNISData(key=self.key, orga=self.orga)

    def transform_data(self, data):
        if self.mapping_key in data:
            data[self.mapping_key] = str(data[self.mapping_key])
        data = super().transform_data(data)
        if self.mapping_key in data:
            data[self.mapping_key] = [data[self.mapping_key]]
        return data


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    possible_event_delivery = [
        "Décision du Collège",
        "décision finale du Collège",
        "décision du Gvt wallon",
        "décision du Collège sur conditions complémentaires",
    ]

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return AddTransitions(key=self.key, orga=self.orga)

    def get_recepisse_check(self, data):
        return "DOSSIER_DATEDEPOT" in data and bool(
            data.get("DOSSIER_DATEDEPOT", False)
        )

    def get_recepisse_date(self, data):
        return data.get("DOSSIER_DATEDEPOT", None)

    def get_delivery_check(self, data):
        return "DOSSIER_DATEDELIV" in data and bool(
            data.get("DOSSIER_DATEDELIV", False)
        )

    def get_delivery_date(self, data):
        return data.get("DOSSIER_DATEDELIV", None)

    def get_delivery_decision(self, data):
        if "wf_transitions" not in data:
            return None
        if data.get("wf_transitions")[0] in ["accepted"]:
            decision = "favorable"
        elif data.get("wf_transitions")[0] in ["refused", "inacceptable"]:
            decision = "defavorable"
        else:
            return None
        return decision

    def handle_failed_check_delivery(self, data):
        if "all_events" not in data:
            return data
        events = []
        for event in data["all_events"]:
            if event["title"] in self.possible_event_delivery:
                events.append(event)
        if len(events) == 0:
            return data
        if "__children__" not in data:
            data["__children__"] = []
        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        new_event = {
            "@type": event_type,
            "urbaneventtypes": event_subtype,
        }
        new_event["decisionDate"] = events[0]["ETAPE_DATEDEPART"]
        new_event["eventDate"] = events[0]["ETAPE_DATEDEPART"]
        data["__children__"].append(new_event)
        return data


class AddEventInDescription(ucore.AddValuesInDescription):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    title = "Evenement"

    def _date_iso_to_string(self, date_iso):
        if date_iso is None:
            return None
        input_date = datetime.fromisoformat(date_iso)
        return input_date.strftime("%d/%m/%Y")

    def get_values(self, data):
        return data.get("all_events", None)

    def handle_value(self, value, data):
        date = self._date_iso_to_string(
            value.get("ETAPE_DATEDEPART", value.get("ETAPE_INS_DATE", None))
        )
        if date is None:
            date = "Pas de date"

        data["description"]["data"] += f"{value['title']} ({date})"

        documents_event = data.get("documents_event", None)
        if documents_event is None:
            return data
        event_id = str(value.get("event_id", None))

        if event_id is None or event_id not in documents_event:
            return data

        document = documents_event[event_id]

        data["description"]["data"] += (
            f": <ul><li>{document['title']} : "
        )

        url = document.get('url', None)
        if url is None:
            url = "Pas de chemin"
        data["description"]["data"] += url

        date_doc = document.get("date_document", None)
        if date_doc is not None:
            data["description"]["data"] += (
                f" - Date document : {self._date_iso_to_string(date_doc)}"
            )

        date_doc_creation = document.get("date_creation", None)
        if date_doc_creation is not None:
            data["description"]["data"] += (
                f" - Date création : {self._date_iso_to_string(date_doc_creation)}"
            )

        data["description"]["data"] += "</li></ul>"

        return data

    def requires(self):
        return AddEvents(key=self.key, orga=self.orga)


class AddOpinionInDescription(ucore.AddValuesInDescription):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    title = "Avis"

    def _date_iso_to_string(self, date_iso):
        if date_iso is None:
            return None
        input_date = datetime.fromisoformat(date_iso)
        return input_date.strftime("%d/%m/%Y")

    def get_values(self, data):
        return data.get("all_opinions", None)

    def handle_value(self, value, data):
        date = self._date_iso_to_string(value.get("AVIS_DATE", None))
        if date is None:
            date = "Pas de date"
        data["description"]["data"] += f"{value['title']} ({date})"
        opnion_state = value.get("AVIS_ETAT", None)
        if opnion_state is None:
            opnion_state = "Pas d'avis"
        
        data["description"]["data"] += f" : {opnion_state}"

        # todo check if there is document for opinion (look in table DOCUMENT_OPINION_VIEW)

        return data

    def requires(self):
        return AddEventInDescription(key=self.key, orga=self.orga)


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return AddOpinionInDescription(key=self.key, orga=self.orga)


class MappingStateToTransition(ucore.UrbanTransitionMapping):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return EventConfigUidResolver(key=self.key, orga=self.orga)


class CreateApplicant(ucore.CreateApplicant):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    mapping_keys = {
        "NOM": "name1",
        "PRENOM": "name2",
        "LOCALITE": "city",
        "ZIP": "zipcode",
        "ADRESSE": "street",
        "TEL": "phone",
        "GSM": "gsm",
    }
    subelement_base = {
        "country": "belgium",
    }

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return MappingStateToTransition(key=self.key, orga=self.orga)


class AddTitle(core.InMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def transform_data(self, data):
        ref = data["reference"]
        data["title"] = f"{ref}"
        subject = data.get("licenceSubject", None)
        if subject:
            data["title"] += f" - {subject}"
        if "__children__" not in data:
            return data
        applicants = ""
        for child in data["__children__"]:
            if child["@type"] == "Applicant":
                applicants += f" - {child['name1']}"
        data["title"] += applicants
        return data

    def requires(self):
        return CreateApplicant(key=self.key, orga=self.orga)


class TransformArchitect(ucore.TransformContact):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    contact_type = "architects"
    data_key = "architects"
    log_failure = True

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def _fix_term(self, street):
        """Fix street with locality in parentheses"""
        street = street.replace("(", " ")
        street = street.replace(")", " ")
        return street.strip()

    def _generate_contact_name(self, data):
        architects = data.get('architects', None)
        if architects is None:
            return None, None
        output = [self._fix_term(architect["ARCHITECT_DES"]) for architect in architects if architect["ARCHITECT_DES"] is not None]
        return output, None

    def requires(self):
        return AddTitle(key=self.key, orga=self.orga)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    subelements_source_key = "addresses"
    subelements_destination_key = "workLocations"
    mapping_keys = {
        "ADRESSE": "street",
        "NUM": "number",
        "ZIP": "zip",
        "LOCALITE": "localite",
    }
    subelement_base = {}

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return TransformArchitect(key=self.key, orga=self.orga)


class TransformWorkLocation(ucore.TransformWorkLocation):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    log_failure = True

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def _fix_street(self, street):
        """Fix street with locality in parentheses"""
        street = street.replace("(", " ")
        street = street.replace(")", " ")
        return street.strip()

    def _fix_localite(self, localite):
        pattern = r".*\s+\((?P<localite>.*)\)"
        match = re.match(pattern, localite)
        if not match:
            return localite
        return match.groupdict()["localite"]

    def _generate_term(self, worklocation, data):
        street = worklocation.get("street", None)
        if not street:
            return None, "Pas de nom de rue présent"
        worklocation["street"] = self._fix_street(street)
        if "localite" in worklocation:
            worklocation["localite"] = self._fix_localite(worklocation["localite"])
        param_values = [
            str(v)
            for k, v in worklocation.items()
            if v and k in ("street", "localite", "zip")
        ]
        return " ".join(param_values), None

    def requires(self):
        return CreateWorkLocation(key=self.key, orga=self.orga)


class CadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", " ET ", "et", "|"]

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def _recursive_split(self, value, separators):
        regexp = f"[{''.join(separators)}]"
        return [v for v in re.split(regexp, value) if v and v not in separators]

    def requires(self):
        return TransformWorkLocation(key=self.key, orga=self.orga)


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    log_failure = True
    division_mapping_path = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    @property
    def mapping_division_dict(self):
        mapping = json.load(open(self.division_mapping_path, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    def _mapping_division(self, data):
        if "division" not in data or data["division"] not in self.mapping_division_dict:
            return "99999"
        return self.mapping_division_dict[data["division"]]

    def requires(self):
        return CadastreSplit(key=self.key, orga=self.orga)

    def _generate_cadastre_dict(self, cadastre, data):
        if cadastre == "Non cadastré":
            return None, ""
        pattern = r"(?P<division>\d{1,4})\s*(?P<section>[a-zA-Z])\s*(?P<radical>\d{0,4})\/?(?P<bis>\d{0,2})\s*(?P<exposant>[a-zA-Z]?)\s*(?P<puissance>\d{0,2})"
        cadastre_split = re.match(pattern, cadastre.strip())
        if not cadastre_split:
            msg = f"Impossible de reconnaitre la parcelle '{cadastre}'"
            return None, msg
        params = cadastre_split.groupdict()
        params["division"] = self._mapping_division(params)
        params["browse_old_parcels"] = True
        return params, None


class AddDocumentInDescription(ucore.AddValuesInDescription):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    title = "Documents Liée"

    def _date_iso_to_string(self, date_iso):
        input_date = datetime.fromisoformat(date_iso)
        return input_date.strftime("%d/%m/%Y")

    def get_values(self, data):
        return data.get("documents", None)

    def handle_value(self, value, data):
        data["description"]["data"] += f"{value['title']} : {value['url']}"

        delivery_date = value.get("delivery_date", None)
        if delivery_date is not None:
            data["description"]["data"] += f" - émis le {self._date_iso_to_string(delivery_date)}"

        create_date = value.get("create_date", None)
        if create_date is not None:
            data["description"]["data"] += f" - créée le {self._date_iso_to_string(create_date)}"

        return data

    def requires(self):
        return TransformCadastre(key=self.key, orga=self.orga)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    drop_keys = [
        "addresses",
        "applicants",
        "WRKDOSSIER_ID",
        "DOSSIER_DATEDELIV",
        "DOSSIER_DATEDEPART",
        "DOSSIER_DATEDEPOT",
        "DOSSIER_REFCOM",
        "DOSSIER_TYPEIDENT",
        "TDOSSIER_OBJETFR",
        "cadastre",
        "zip",
        'all_events',
        'all_opinions',
        'documents',
        "documents_event"
    ]

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return AddDocumentInDescription(key=self.key, orga=self.orga)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "acropole"
    key = luigi.Parameter()
    orga = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return DropColumns(key=self.key, orga=self.orga)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "acropole"
    export_filepath = luigi.Parameter()
    key = luigi.Parameter()
    orga = luigi.Parameter()

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = f"./failures/{self.orga}-" f"{self.__class__.__name__}/{fname}.json"
        return luigi.LocalTarget(fpath)

    def requires(self):
        return ValidateData(key=self.key, orga=self.orga)
