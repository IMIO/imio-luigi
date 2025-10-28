# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools
from imio_luigi.urban.address import find_address_match

import copy
import json
import logging
import luigi
import numpy as np
import os
import pandas as pd
import re


logger = logging.getLogger("luigi-interface")


class GetFromCSV(core.GetFromCSVFile):
    task_namespace = "mouscron"
    filepath = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    delimiter = ";"
    dtype = "string"

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
                yield Transform(key=row["numero_permis"], data=row)
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
    task_namespace = "mouscron"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    log_failure = False
    whitelist = [
        "utilisateur_fk",
        "type_permis_fk",
        "organisme_fk",
        "localite_fk",
        "division_fk",
        "parcelle_info_fk",
        "civilite_fk",
        "pays_fk",
        "rue_fk",
    ]

    @property
    def get_fk_table_mapping(self):
        with open("./config/mouscron/fk_table_mapping-mouscron.json", "r") as f:
            data = json.load(f)
        return data

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            __import__("pdb").set_trace()
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

    def get_table(self, table, id, key, orient="dict"):
        folder = table[0]
        pdata = pd.read_csv(
            f"./data/mouscron/{table}", delimiter=";", dtype="string"
        )
        pdata = pdata.replace({np.nan: None})
        result = pdata.loc[pdata[key] == id]
        if orient == "dict" and len(result) == 1:
            return result.squeeze().to_dict()
        return result.to_dict(orient=orient)

    def add_outside_data(self, data, table, key, name=None):
        values = self.get_table(table, data["id"], key, orient="records")
        values = [
            self.populate_cross_data(value, whitelist=self.whitelist)
            for value in values
        ]
        key_name = table
        if name:
            key_name = name

        data[key_name] = values

        return data

    def populate_cross_data(self, data, blacklist=[], whitelist=[]):
        if type(data) == str:
            return data
        for key in data:
            if type(data[key]) is dict or key == "permis_fk" or key in blacklist:
                continue
            if key in self.get_fk_table_mapping and data[key]:
                table = self.get_fk_table_mapping[key]["table"]
                key_table = self.get_fk_table_mapping[key]["key"]
                value = self.get_table(table, data[key], key_table)
                data[key] = self.populate_cross_data(value, whitelist=self.whitelist)
        return data

    def extract_data(self, data, from_path, to):
        result = utils.get_value_from_path(data, from_path)
        if not result:
            return data
        data[to] = result
        return data

    def run(self):
        try:
            with self.output().open("w") as f:
                data = dict(self.data)
                # if self.key == "2018/159":
                #     __import__('pdb').set_trace()
                data = self.populate_cross_data(data, whitelist=self.whitelist)
                data = self.add_outside_data(data, "p/p_demandeur.csv", "permis_fk", name="p_demandeur")
                data = self.add_outside_data(data, "p/p_parcelle.csv", "permis_fk", name="p_parcelle")
                # data = self.add_outside_data(data, "p_echeancier", "permis_fk")
                # data = self.add_outside_data(data, "p_document_info_permis", "permis_fk")
                # data = self.add_outside_data(data, "p_parcelle_lotissement", "permis_fk")
                data = self.add_outside_data(
                    data, "p/p_permis_adresse_commune.csv", "permis_fk", name="p_permis_adresse_commune"
                )
                # data = self.add_outside_data(data, "p_permis_dyn_value", "permis_fk")
                # data = self.add_outside_data(data, "p_services_a_consulter", "permis_fk")
                # data = self.add_outside_data(data, "p_permis_avis_fonctionnaire", "permis_fk")
                data = self.extract_data(data, "type_permis_fk/valeur", "@type")
                # data = self.extract_data(data, "utilisateur_fk/login", "foldermanagers")
                data = self.extract_data(data, "organisme_fk/nom", "architecte")
                data = self.extract_data(data, "nature_fk/libelle_f", "libnat_alt")
                f.write(json.dumps(data))
            yield WriteToJSON(key=self.key)
        except Exception as e:
            self._handle_exception(data, e, f)


class ValueCleanup(core.ValueFixerInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    rules_filepath = "./config/mouscron/fix-mouscron.json"

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    mapping = {
        "numero_permis": "reference",
        "libnat": "licenceSubject",
        "statut": "wf_transitions",
    }

    def transform_data(self, data):
        data = super().transform_data(data)
        if "licenceSubject" in data and data["licenceSubject"] is None:
            if "libnat_alt" in data and data["libnat_alt"]:
                data["licenceSubject"] = data["libnat_alt"]
            else:
                del data["licenceSubject"]
        return data

    def requires(self):
        return ValueCleanup(key=self.key)


class MakeTitle(core.InMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()

    def _make_title(self, data):
        title = ""
        ref = data.get("reference", None)
        if ref:
            title += ref
        object = data.get("licenceSubject", None)
        if object:
            title += f" - {object}"

        return title

    def transform_data(self, data):
        data["title"] = self._make_title(data)
        return data

    def requires(self):
        return Mapping(key=self.key)


class ConvertDates(core.ConvertDateInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    keys = (
        "cre_date",
        "date_demande",
        "date_recepisse",
        "date_cloture",
        "annonce_projet_fk/date_debut_affichage",
        "annonce_projet_fk/echeance_debut_reclamations_fk/date_debut",
        "annonce_projet_fk/echeance_fin_affichage_fk/date_fin",
        "enquete_publique_eu_fk/date_debut",
        "enquete_publique_eu_fk/date_fin"
        "enquete_publique_fk/date_debut",
        "enquete_publique_fk/date_fin",
        "autorisation_cu_fk/date_avis_college",
        "autorisation_fk/date_avis_college",
        "decision_environnement_fk/date_avis_college",
        "decision_fk/date_avis_college",
        "decision_unique_fk/date_avis_college",
        "autorisation_codt_fk/date_decision_college",
        "autorisation_codt_fk/date_decision_gvt_suspension",
        "autorisation_codt_fk/date_decision_saisine_gvt",
        "decision_environnement_classe3_fk/date_decision",
        "decision_environnement_fk/date_decision_autorite",
        "decision_environnement_fk/date_avis_college",
        "decision_unique_fk/date_decision_autorite",
    )
    date_format_input = "%Y-%m-%d"
    date_format_output = "%Y-%m-%dT%H:%M:%S"
    log_failure = True

    def transform_data(self, data):
        cre_date = data.get("cre_date", None)
        if not cre_date:
            return super().transform_data(data)

        match = re.match(r"(?P<date>\d{4}-\d{2}-\d{2}).*", cre_date)
        if not match:
            return super().transform_data(data)

        data["cre_date"] = match.groupdict()["date"]

        return super().transform_data(data)

    def requires(self):
        return MakeTitle(key=self.key)


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    filepath = "./config/mouscron/add-data-mouscron.json"

    def requires(self):
        return ConvertDates(key=self.key)


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    mapping_filepath = "./config/mouscron/mapping-type-mouscron.json"
    mapping_key = "@type"
    codt_trigger = [
        "UrbanCertificateOne",
        "NotaryLetter",
        "UniqueLicence",
        "IntegratedLicence",
        "Article127"
    ]
    codt_start_date = datetime(2017, 6, 1)
    public_lic_triggers = [
        "ville de mouscron",
        "administration communale",
        "domaine de la societe publique d'administration des batiments scolaires du hainaut",
        "société publique d'administration des bâtiments scolaires du hainaut",
        "ieg",
        "ores-asset",
        "ores asset",
        "cpas",
        "slm",
        "société de logements de mouscron"
    ]

    def hanlde_article127(self, data):
        date_autorisation_tutelle = utils.get_value_from_path(
            data, "autorisation_fk/date_autorisation_tutelle"
        )
        if date_autorisation_tutelle:
            data["@type"] = "Article127"
        return data

    def hanlde_public_licence(self, data):
        applicants = data.get("p_demandeur", None)
        if applicants is None:
            return data
        for applicant in applicants:
            applicant_name = applicant.get("name", None)
            if applicant_name is None:
                return data
            for trigger in self.public_lic_triggers:
                if trigger in applicant_name.lower():
                    data["@type"] = "Article127"
        return data

    def handle_unique_integrated_licence(self, data):
        p_fonctionnaire_delegue = data.get("p_fonctionnaire_delegue", None)
        if p_fonctionnaire_delegue is None:
            return data
        date_envoi = p_fonctionnaire_delegue.get("date_envoi", None)
        if date_envoi is None:
            return data
        reference = data.get("reference", None)
        for ref in ["/pu", "-pu"]:
            if ref in reference.lower():
                data["@type"] = "UniqueLicence"
        if "piur" in reference.lower():
            data["@type"] = "IntegratedLicence"
        return data

    def transform_data(self, data):
        data = super().transform_data(data)
        data = self.hanlde_article127(data)
        data = self.hanlde_public_licence(data)
        data = self.handle_unique_integrated_licence(data)

        date = data.get("date_demande", None)
        if not date:
            date = data.get("cre_date", None)
        if not date:
            return data

        date = datetime.fromisoformat(date)

        if data["@type"] in self.codt_trigger and date > self.codt_start_date:
            data["@type"] = f"CODT_{data['@type']}"

        if data["@type"] == "CODT_CommercialLicence" and date < self.codt_start_date:
            data["@type"] = "IntegratedLicence"

        return data

    def requires(self):
        return AddExtraData(key=self.key)


class AddNISData(ucore.AddNISData):
    task_namespace = "mouscron"
    key = luigi.Parameter()

    def requires(self):
        return MappingType(key=self.key)


class AddTransitions(core.MappingValueWithFileInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    mapping_filepath = "./config/mouscron/mapping-statut-mouscron.json"
    mapping_key = "wf_transitions"

    @property
    @core.utils._cache(ignore_args=True)
    def mapping(self):
        mapping = json.load(open(self.mapping_filepath, "r"))
        return {l["key"]: l["value"] for l in mapping["keys"]}

    def requires(self):
        return AddNISData(key=self.key)

    def transform_data(self, data):
        data = super().transform_data(data)
        data[self.mapping_key] = [data[self.mapping_key]]
        return data


class AddEvents(ucore.AddUrbanEvent):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    delivery_config = None

    def requires(self):
        return AddTransitions(key=self.key)

    # recipisse
    def get_recepisse_check(self, data):
        return "date_demande" in data or data["date_demande"]

    def get_recepisse_date(self, data):
        return data["date_demande"]

    def handle_numeral_descision(self, value, data):
        mapping = {
            "EnvClassTwo" : {"0": "octroi", "1": "refus"},
            "default" : {"0": "favorable", "1": "defavorable"}
        }
        return mapping.get(data["@type"], mapping["default"]).get(str(value), None)

    def handle_avis_descision(self, value, data):
        mapping = {
            "EnvClassTwo" : {
                "Favorable": "octroi",
                "Favorable et Abstention": "octroi",
                "Favorable Conditionné": "octroi",
                "Réputé Favovable": "octroi",
                "Défavorable": "refus",
                "Refus direct": "refus",
            },
            "default" : {
                "Favorable": "favorable",
                "Favorable et Abstention": "favorable",
                "Favorable Conditionné": "favorable",
                "Réputé Favovable": "favorable",
                "Défavorable": "defavorable",
                "Refus direct": "defavorable",
            }
        }
        return mapping.get(data["@type"], mapping["default"]).get(value, None)

    def handle_transition_descision(self, _, data):
        if data.get("wf_transitions")[0] in ["accepted"]:
            decision = "favorable"
        elif data.get("wf_transitions")[0] in ["refused", "inacceptable"]:
            decision = "defavorable"
        else:
            return None
        return decision

    # delivery
    def get_delivery_check(self, data):
        columns = [
            {#autorisation_codt_fk
                "date": "autorisation_codt_fk/date_decision_college",
                "decision": "autorisation_codt_fk/decision_college_",
                "decision_adapter": self.handle_numeral_descision
            },
            {
                "date": "autorisation_codt_fk/date_decision_gvt_suspension",
                "decision": "autorisation_codt_fk/decision_gvt_suspension_",
                "decision_adapter": self.handle_numeral_descision
            },
            {
                "date": "autorisation_codt_fk/date_decision_saisine_gvt",
                "decision": "autorisation_codt_fk/decision_saisine_fd",
                "decision_adapter": self.handle_numeral_descision
            },

            {#autorisation_fk
                "date": "autorisation_fk/date_avis_college",
                "decision": "autorisation_fk/avis_college_fk/libelle_f",
                "decision_adapter": self.handle_avis_descision
            },
            {
                "date": "autorisation_fk/date_avis_college",
                "decision": "autorisation_fk/avis_fk/libelle_f",
                "decision_adapter": self.handle_avis_descision
            },

            {#decision_environnement_classe3_fk
                "date": "decision_environnement_classe3_fk/date_decision",
                "decision": "decision_environnement_classe3_fk/decision",
                "decision_adapter": self.handle_numeral_descision
            },

            {#decision_environnement_fk
                "date": "decision_environnement_fk/date_decision_autorite",
                "decision": "decision_environnement_fk/decision_autorite",
                "decision_adapter": self.handle_numeral_descision
            },
            {
                "date": "decision_environnement_fk/date_avis_college",
                "decision": "decision_environnement_fk/avis_college_fk/libelle_f",
                "decision_adapter": self.handle_avis_descision
            },

            {#decision_unique_fk
                "date": "decision_unique_fk/date_decision_autorite",
                "decision": "decision_unique_fk/decision_autorite",
                "decision_adapter": self.handle_numeral_descision
            },
            {
                "date": "decision_unique_fk/date_avis_college",
                "decision": "decision_unique_fk/avis_college_fk/libelle_f",
                "decision_adapter": self.handle_avis_descision
            },

            {#decision_fk
                "date": "decision_fk/date_decision_autorite",
                "decision": "decision_fk/decision_autorite",
                "decision_adapter": self.handle_numeral_descision
            },

            {#date_cloture
                "date": "date_cloture",
                "decision": "wf_transitions",
                "decision_adapter": self.handle_transition_descision
            }
        ]
        for column in columns:
            if utils.get_value_from_path(data, column["date"]) and utils.get_value_from_path(data, column["decision"]):
                self.delivery_config = column
                return True
        return False

    def get_delivery_date(self, data):
        date_config = self.delivery_config["date"]
        return utils.get_value_from_path(data, date_config)

    def get_delivery_decision(self, data):
        decision_path = self.delivery_config["decision"]
        decision_adapter = self.delivery_config["decision_adapter"]
        decision = utils.get_value_from_path(data, decision_path)
        return decision_adapter(decision, data)


class AddOtherEvent(ucore.AddEvents):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    event_config = {
        "rapport": {
            "parents_keys": ["autorisation_cu_fk", "autorisation_fk", "decision_environnement_fk", "decision_fk", "decision_unique_fk"],
            "check_key": ["date_avis_college"],
            "date_mapping": {"decisionDate": "date_avis_college", "eventDate": "date_avis_college"},
            "decision_mapping": {"decision": "avis_college_fk/libelle_f"},
            "decision_value_mapping": {
                "decision" : {
                    "Attente": "favorable-defaut",
                    "Abstention": "favorable-defaut",
                    "Conditionné": "favorable-conditionnel",
                    "Défavorable": "defavorable",
                    "Favorable": "favorable",
                    "Favorable et Abstention": "favorable",
                    "Favorable Conditionné": "favorable",
                    "Favorable & Défavorable": "favorable-conditionnel",
                    "Réputé Favovable": "favorable",
                    "s'abstient": "favorable-defaut",
                }
            },
            "mapping": {
                "Article127": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "BuildLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "CODT_BuildLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "CODT_CommercialLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "CODT_IntegratedLicence": {"urban_type": "rapport-synthese", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "CODT_ParceloutLicence": {"urban_type": "copy_of_rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "CODT_UniqueLicence": {"urban_type": "rapport-synthese", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "EnvClassTwo": {"urban_type": "rapport-synthese", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "IntegratedLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "ParcelOutLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "UniqueLicence": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
                "UrbanCertificateTwo": {"urban_type": "rapport-du-college", "date": ["eventDate", "decisionDate"], "decision": "decision"},
            }
        },
        "enquete": {
            "parents_keys": ["enquete_publique_eu_fk", "enquete_publique_fk"],
            "check_any_key": ["date_debut", "date_fin"],
            "date_mapping": {"investigationStart": "date_debut", "investigationEnd": "date_fin"},
            "mapping": {
                "Article127": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart", "investigationEnd"]},
                "BuildLicence": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart", "investigationEnd"]},
                "CODT_BuildLicence": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_ParcelOutLicence": {"@type": "UrbanEventInquiry", "urban_type": "copy_of_enquete-publique-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_UrbanCertificateTwo": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart", "investigationEnd"]},
                "ParcelOutLicence": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart", "investigationEnd"]},
                "UrbanCertificateTwo": {"@type": "UrbanEventInquiry", "urban_type": "enquete-publique", "date": ["investigationStart", "investigationEnd"]},
            }
        },
        "annonce": {
            "parents_keys": ["annonce_projet_fk"],
            "check_any_key": ["date_debut_affichage", "echeance_debut_reclamations_fk/date_debut", "echeance_fin_affichage_fk/date_fin"],
            "date_mapping": {"investigationStart": ["date_debut_affichage", "echeance_debut_reclamations_fk/date_debut"], "investigationEnd": "echeance_fin_affichage_fk/date_fin"},
            "mapping": {
                "CODT_Article127": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_BuildLicence": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_CommercialLicence": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_IntegratedLicence": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_ParceloutLicence": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_UrbanCertificateOne": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet-codt", "date": ["investigationStart", "investigationEnd"]},
                "CODT_UrbanCertificateTwo": {"@type": "UrbanEventAnnouncement", "urban_type": "annonce-de-projet", "date": ["investigationStart", "investigationEnd"]},
            }
        },
        "voirie": {
            "check_any_key": ["voirie_fk/date_decision_voirie"],
            "date_mapping": { "eventDate": "voirie_fk/date_decision_voirie"},
            "mapping": {
                "Article127" : {"urban_type": "passage-conseil-communal", "date": ["eventDate"]},
                "BuildLicence" : {"urban_type": "passage-conseil-communal", "date": ["eventDate"]},
                "EnvClassOne" : {"urban_type": "township-council", "date": ["eventDate"]},
                "EnvClassTwo" : {"urban_type": "township-council", "date": ["eventDate"]},
                "IntegratedLicence" : {"urban_type": "passage-conseil-communal", "date": ["eventDate"]},
                "ParceloutLicence" : {"urban_type": "passage-conseil-communal", "date": ["eventDate"]},
                "UniqueLicence" : {"urban_type": "passage-conseil-communal", "date": ["eventDate"]},
            }
        },
        "avis_college": {
            "check_any_key": [
                "decision_fk/date_avis_college",
                "decision_unique_fk/date_avis_college",
                "decision_environnement_fk/date_avis_college",
                "decision_environnement_classe3_fk/date_prise_acte_college",
                "autorisation_fk/date_avis_college",
                "autorisation_cu_fk/date_avis_college"
            ],
            "date_mapping": {
                "eventDate": ["*"],
                "decisionDate": ["*"]
            },
            "decision_mapping": {
                "externalDecision": [
                    "autorisation_fk/avis_college_fk/libelle_f",
                    "autorisation_fk/avis_fk/libelle_f",
                    "autorisation_cu_fk/avis_college_fk/libelle_f",
                    "autorisation_cu_fk/avis_fk/libelle_f"
                ],
                "decision": [
                    "decision_fk/decision_autorite",
                    "decision_unique_fk/decision_autorite",
                    "decision_environnement_fk/decision_autorite",
                    "decision_environnement_classe3_fk/decision"
                ]
            },
            "decision_value_mapping": {
                "externalDecision": {
                    "Attente": "favorable-defaut",
                    "Abstention": "favorable-defaut",
                    "Conditionné": "favorable-conditionnel",
                    "Défavorable": "defavorable",
                    "Favorable": "favorable",
                    "Favorable et Abstention": "favorable",
                    "Favorable Conditionné": "favorable",
                    "Favorable & Défavorable": "favorable-conditionnel",
                    "Réputé Favovable": "favorable",
                    "s'abstient": "favorable-defaut",
                },
                "decision": {
                    "0": "favorable",
                    "2": "defavorable",
                }
            },
            "mapping": {
                "CODT_IntegratedLicence" :  {"urban_type": "avis-prealable-college", "date": ["eventDate"], "decision": "externalDecision"},
                "CODT_UniqueLicence" :  {"urban_type": "avis-college", "date": ["decisionDate"], "decision": "externalDecision"},
                "Declaration" :  {"urban_type": "deliberation-college", "date": ["eventDate"], "decision": "decision"},
            }
        }
    }

    def requires(self):
        return AddEvents(key=self.key)


class EventConfigUidResolver(ucore.UrbanEventConfigUidResolver):
    task_namespace = "mouscron"
    key = luigi.Parameter()

    def requires(self):
        return AddOtherEvent(key=self.key)


class MappingStateToTransition(ucore.UrbanTransitionMapping):
    task_namespace = "mouscron"
    key = luigi.Parameter()

    def requires(self):
        return EventConfigUidResolver(key=self.key)


class CreateApplicant(ucore.CreateApplicant):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    subelement_container_key = "__children__"
    mapping_keys = {
        "nom": "name1",
        "prenom": "name2",
        "civilite_fk/libelle_f": "title",
        "pays_fk/code_pays": "country",
        "localite": "city",
        "rue": "street",
        "numero": "number",
        "code_postal": "zipcode",
        "societe": "society",
        "fax": "fax",
        "gsm": "gsm",
        "telephone": "phone",
        "mail": "email",
    }
    subelement_base = {}

    def _get_values(self, applicant):
        output = {}
        for key, destination in self.mapping_keys.items():
            value = utils.get_value_from_path(applicant, key)
            if not value:
                continue
            output[destination] = value
        if "name1" not in output:
            output["name1"] = applicant["prenom"] or applicant["localite"] or ""
        return output

    def _fix_country(self, new_element):
        country_code = new_element.get("country", None)
        if not country_code:
            return new_element

        country_mapping = {
            "BE": "belgium",
            "DE": "germany",
            "FR": "france",
            "LU": "luxembourg",
            "NL": "netherlands",
        }

        new_element["country"] = country_mapping.get(country_code, country_code)

        return new_element

    def _complete_title(self, data, applicant):
        title = data.get("title", None)
        if not title:
            return data
        name1 = applicant.get("name1", None)
        if not name1:
            return data
        title += f" - {name1}"
        name2 = applicant.get("name2", None)
        if not name2:
            data["title"] = title
            return data
        title += f" {name2}"
        data["title"] = title
        return data

    def transform_data(self, data):
        self.apply_subelement_base_type(data)
        applicants = data.get("p_demandeur", None)
        if not applicants:
            return data

        if "__children__" not in data:
            data["__children__"] = []

        for applicant in applicants:
            new_element = copy.deepcopy(self.subelement_base)
            new_element = new_element | self._get_values(applicant)
            new_element = self._fix_country(new_element)

            data = self._complete_title(data, new_element)

            data["__children__"].append(new_element)

        return data

    def requires(self):
        return MappingStateToTransition(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    subelements_source_key = "p_permis_adresse_commune"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}
    log_failure = True

    def _remove_end_parenthesis(self, element):
        pattern = r"(?P<street>.*)(?P<letter>\([A-Z]\))$"
        find = re.search(pattern, element)
        if not find:
            return element
        groups = find.groupdict()
        if "street" not in groups:
            return element
        return groups["street"]

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
            if element['info_rue_f'] is None:
                continue
            new_element = {
                "street": f"{self._remove_end_parenthesis(element['info_rue_f'])} ({element['localite_fk']['code_postal']} - {element['localite_fk']['libelle_f']})"
            }
            number = element.get("numero", None)
            if number is None:
                number = ""
            boite = element.get("boite", None)
            if boite is not None:
                number = f"{number} {boite}"
            new_element["number"] = number
            data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformWorkLocation(ucore.TransformWorkLocation):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CreateWorkLocation(key=self.key)

    def generate_street_item(self, street_uid, number):
        if not number:
            return [{"street": street_uid, "number": ""}]
        if "-" in number:
            numbers = number.split("-")
            if len(numbers) == 2 and isinstance(numbers[0], int) and isinstance(numbers[1], int):
                numbers = [number for number in range(int(numbers[0]), int(numbers[1]))]
        elif "/" in number:
            numbers = number.split("/")
        else:
            numbers = [number]

        return [
            {
                "street": street_uid,
                "number": str(number)
            }
            for number in list(set(numbers))
        ]

    def _generate_term(self, worklocation, data):
        return worklocation["street"], None


class TransformCadastre(ucore.TransformCadastre):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    log_failure = True
    cadastre_key = "p_parcelle"
    mapping_division_dict = {
        "1" : "54007",
        "2" : "54432",
        "3" : "54433",
        "4" : "54434",
        "5" : "54435",
        "6" : "54436",
        "7" : "54003",
        "8" : "54004",
        "9" : "54006"
    }

    def requires(self):
        return TransformWorkLocation(key=self.key)

    def _generate_cadastre_dict(self, cadastre, data):
        capkey = cadastre.get("cadastre", None)
        if capkey is None:
            return None, "Pas de numeros de cadastre"
        pattern = r"(?P<radical>\d{4})\/(?P<bis>\d{2})(?P<exposant>[A-Z#]{1})(?P<puissance>\d{3})"
        match = re.match(pattern, capkey)
        if not match:
            return None, f"Cadastre non reconnu : {capkey}"
        output = match.groupdict()
        new_output = {}
        for key, value in output.items():
            try:
                value = int(value)
                if value == 0:
                    value = ""
            except ValueError:
                value = value
            if value == "#":
                value = ""
            new_output[key] = str(value).upper()
        section = cadastre.get('section_cadastrale', None)
        if section is not None:
            new_output["section"] = str(section).upper()
        division = cadastre.get('division_fk', None)
        if division is not None:
            code_ins = division.get("code_ins", None)
            if code_ins is None:
                numero = division.get("numero", None)
                if numero is None:
                    code_ins = ""
                else:
                    code_ins = self._mapping_division(str(numero))
            new_output["division"] = code_ins
        new_output["original_cadastre"] = capkey
        return new_output, None


class TransformArchitect(core.GetFromRESTServiceInMemoryTask):
    task_namespace = "mouscron"
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

    def fix_architect_mapping(self, name):
        mapping = {
            "Atelier 3A S.P.R.L" : "2be22db5cffd40878235a3edc4522a19",
            "ATELIER D'ARCHITECTURE 3A": "2be22db5cffd40878235a3edc4522a19",
            "DE DEURWAERDER Hélène": "08ebfd5d6135465e957eab5e4934aa5f",
            "DEWULF Jean-Luc": "4dfc3efddcaa4a4b980d3ec8db40aef2",
            "DUPONCHEEL Jean": "26bfd98d0a3e4873a0847c00f30e9d48",
            "MENTEN J-P" : "8237d8dd9098419d80559386f2237585",
            "WINDELS Jean et paul": "5ec4e9c708e34edcb1c680c9e583edda"
        }
        if name not in mapping:
            return None
        return mapping[name]

    def transform_data(self, data):
        errors = []
        type_list = utils.get_value_from_path(data, "organisme_fk/type_list")
        if type_list and type_list != "ARCHITECTE":
            return data, errors
        architecte_name = utils.get_value_from_path(data, "organisme_fk/nom")
        if not architecte_name:
            return data, errors
        architecte_fname = utils.get_value_from_path(data, "organisme_fk/prenom")
        if architecte_fname and architecte_fname != ".":
            architecte_name = f"{architecte_name} {architecte_fname}"
        fix_uid_mapping = self.fix_architect_mapping(architecte_name)
        if fix_uid_mapping:
            data["architects"] = [fix_uid_mapping]
            return data, errors
        params = {"SearchableText": f"{utils.fix_search_term(architecte_name)}", "metadata_fields": "UID"}
        r = self.request(parameters=params)
        if r.status_code != 200:
            errors.append(f"Response code is '{r.status_code}', expected 200")
            return data, errors
        result = r.json()
        if result["items_total"] == 0:
            errors.append(f"Aucun résultat pour l'architecte: '{architecte_name}'")
            return data, errors
        elif result["items_total"] > 1:
            value, error = utils.find_result_similarity(result["items"], architecte_name, key="title")
            if isinstance(error, float):
                error = f"Attention, la ressemblance entre les noms des architect trouvé n'est de que {(error)*100}%"
            if not value:
                errors.append(f"Plusieurs résultats pour l'architecte: '{architecte_name}'")
                return data, errors
            if error:
                errors.append(error)
            data["architects"] = [value["UID"]]
        else:
            data["architects"] = [result["items"][0]["UID"]]
        return data, errors


class AddEventInDescription(ucore.AddValuesInDescription):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    event_list_path = "./config/mouscron/config_all_events.json"
    title = "Les évenements"

    @property
    def event_list(self):
        with open(self.event_list_path, "r") as f:
            output = json.load(f)
        return output

    def get_values(self, data):
        outputs = [
            {
                "key": key,
                "value": self.get_sub_values(utils.get_value_from_path(data, key), self.event_list[key])
            }
            for key, value in data.items()
            if key in self.event_list and value is not None
        ]
        return [
            output
            for output in outputs
            if len(output["value"]) > 0
        ]

    def get_sub_values(self, data, config):
        all_keys_config = []
        for key, value in config.items():
            if key.endswith("_keys") and isinstance(value, list):
                all_keys_config += value
        return {
            key: utils.get_value_from_path(data, key)
            for key in all_keys_config
            if utils.get_value_from_path(data, key) is not None
        }

    def handle_key_title(self, key):
        if key.endswith("/libelle_f"):
            key = key.replace("/libelle_f", "")
        if key.endswith("_fk"):
            key = key.replace("_fk", "")
        key = key.replace("_", " ")
        return key.capitalize()

    def pretify_date(self, date):
        try:
            iso_date = datetime.fromisoformat(date[:10])
            export_date = iso_date.strftime("%d/%m/%Y")
        except Exception:
            export_date = f"Erreur avec la date : {date}"

        return export_date

    def handle_date(self, value, config):
        dates = [
            f"<li>{self.handle_key_title(date)} : {self.pretify_date(value[date])}</li>"
            for date in config
            if value.get(date, None) is not None
        ]
        if len(dates) < 1:
            return None
        joiner = "\n"
        return f"<li>Les dates :\n<ul>{joiner.join(dates)}</ul></li>"

    def handle_avis(self, value, config):
        avis_list = [
            f"<li>{self.handle_key_title(key)} : {utils.get_value_from_path(value, key)}</li>" 
            for key in config
            if utils.get_value_from_path(value, key) is not None
        ]
        if len(avis_list) < 1:
            return None
        joiner = "\n"
        return f"<li>Les avis :\n<ul>{joiner.join(avis_list)}</ul></li>"

    def handle_decision(self, value, config):
        decisions = [
            f"<li>{self.handle_key_title(key)} : {value[key]}</li>"
            for key in config
            if value.get(key, None) is not None
        ]
        if len(decisions) < 1:
            return None
        joiner = "\n"
        return f"<li>Les décisions :\n<ul>{joiner.join(decisions)}</ul></li>"

    def handle_value(self, value, data):
        key = value["key"]
        value = value["value"]
        config = self.event_list[key]
        title = config["title"]
        output_text = []
        if "date_keys" in config:
            result = self.handle_date(
                value=value,
                config=config["date_keys"]
            )
            if result is not None:
                output_text.append(result)

        if "avis_keys" in config:
            result = self.handle_avis(
                value=value,
                config=config["avis_keys"]
            )
            if result is not None:
                output_text.append(result)

        if "decision_keys" in config:
            result = self.handle_decision(
                value=value,
                config=config["decision_keys"]
            )
            if result is not None:
                output_text.append(result)
        if len(output_text) > 0:
            joiner = "\n"
            data["description"]["data"] += f"{title} :\n<ul>{joiner.join(output_text)}</ul></li>"
        return data

    def requires(self):
        return TransformArchitect(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    drop_keys = [
        "actif",
        "agent_technique_fk",
        "annee_recepisse",
        "annonce_projet_fk",
        "architecte",
        "article127",
        "article127_travaux_impact_limite",
        "assainissement_fk",
        "audition_souhaitee",
        "audition_souhaitee_fk",
        "auteur_constat_infraction",
        "auteur_demande",
        "auteur_demande_",
        "auteur_division",
        "autorisation_codt_fk",
        "autorisation_cu_fk",
        "autorisation_fk",
        "autorisation_notaire_fk",
        "autorite_competente",
        "autorite_competente_",
        "autorite_competente_premiere_instance",
        "categorie_etablissement",
        "cessation_fk",
        "charge_imposee_fk",
        "classe_etablissement",
        "classement",
        "code_carto",
        "code_urbain_fk",
        "commission_consultative_fk",
        "completude_fk",
        "constatant_fk",
        "contenance",
        "cre_date",
        "cre_user",
        "date_abrogation",
        "date_abrogation_valeur_reglementaire",
        "date_accuse",
        "date_autorisation",
        "date_classement",
        "date_cloture",
        "date_constat",
        "date_demande",
        "date_demande_payement",
        "date_demande_traitee",
        "date_depot",
        "date_echeance",
        "date_echeance_accuse_reception",
        "date_echeance_commune",
        "date_echeance_paiement_depot",
        "date_echeance_payement",
        "date_echeance_reponse",
        "date_envoi_accuse_reception_hors_delai",
        "date_envoi_commune",
        "date_envoi_demande_observations",
        "date_envoi_urbain",
        "date_modification",
        "date_notification_peremption",
        "date_paiement_depot",
        "date_passage_acte",
        "date_payement",
        "date_permis_annule_abandon",
        "date_permis_irrecevable",
        "date_plainte",
        "date_pv",
        "date_recepisse",
        "date_recepisse2",
        "date_reception_commune",
        "date_reception_decision_par_exploitant",
        "date_reception_recommande",
        "date_reponse",
        "date_reunion",
        "date_statut",
        "date_suivi",
        "date_taxe",
        "date_transmis_pv_reunion",
        "date_visite",
        "date_visite_controle",
        "decision_environnement_classe3_fk",
        "decision_environnement_fk",
        "decision_fk",
        "decision_unique_fk",
        "delai",
        "dernier_calcul_echeances",
        "derogation_architecte",
        "destination_bien",
        "directive_fk",
        "domaine_public",
        "dtype",
        "ech_observation_exploitant_fk",
        "ech_peremption_fk",
        "echeance_envoi_irrecevable_fk",
        "enquete_publique_eu_fk",
        "enquete_publique_fk",
        "environnement_classe_fk",
        "environnement_type_etablissement_fk",
        "envoi_cadastre",
        "etude_incidence_fk",
        "fonctionnaire_delegue_fk",
        "fonctionnaire_technique_fk",
        "from_permis_lotir_id",
        "habitation",
        "heure_reunion",
        "heure_visite_controle",
        "id",
        "info_complementaire",
        "infraction_fk",
        "instruction_codt_fk",
        "instruction_infraction_urbanistique_fk",
        "instruction_insalubrite_logement_fk",
        "instruction_permis_location_fk",
        "instruction_reclamation_plainte_fk",
        "instruction_reunion_fk",
        "key",
        "libnat",
        "libnat_alt",
        "libnat_nl",
        "lieu_affichage",
        "lot_no",
        "mod_date",
        "mod_user",
        "montant_depot",
        "montant_payement",
        "motif_permis_annule_abandon",
        "motif_permis_irrecevable",
        "nature_etablissement",
        "nature_fk",
        "nb_construction",
        "nb_logement",
        "nom_lotissement",
        "nombre_de_lots",
        "nombre_de_lots_modification",
        "notaire_type_acte_fk",
        "notaire_type_dossier_fk",
        "notaire_type_payement_fk",
        "notification_decision_aexploitant",
        "numero_permis_agora",
        "numero_permis_delivre",
        "numero_pv_eventuel",
        "observation",
        "organisme_fk",
        "origine_permis",
        "p_demandeur",
        "p_parcelle",
        "p_permis_adresse_commune",
        "parcelle_hors_commune",
        "performance_energetique_batiment_fk",
        "plaignant_conseil_college",
        "plaignant_media",
        "plaignant_service_communal",
        "plaignant_voisin",
        "prorogation",
        "rapport_technique_fk",
        "recevabilite_fk",
        "recour_eu_fk",
        "recour_fk",
        "ref_dgo6",
        "reference_courrier_notaire",
        "reference_demandeur",
        "reference_interne",
        "reference_lotissement",
        "reference_urbanisme",
        "references_anciens_permis",
        "remarque_cession",
        "remarque_registre_modification",
        "remarque_resume",
        "remarque_service_consulte",
        "resume_eu_fk",
        "scn_nette",
        "situation_travaux",
        "statistique",
        "statut",
        "suivi",
        "surface_fk",
        "taxe",
        "travaux_fk",
        "type_constat_fk",
        "type_demande_condition",
        "type_depot",
        "type_dossier_fk",
        "type_dossier_lotissement",
        "type_paiement_fk",
        "type_permis_fk",
        "type_permis_premiere_instance",
        "type_permis_reunion_fk",
        "urbanisme_fk",
        "utilisateur_fk",
        "valeur_rue",
        "voirie_fk",
        "voirie_rt_fk",
        "p_permis_avis_fonctionnaire",
        "Unnamed: 0",
    ]

    def requires(self):
        return AddEventInDescription(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"
    log_failure = True

    def requires(self):
        return DropColumns(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "mouscron"
    export_filepath = "./results/result-mouscron"
    key = luigi.Parameter()

    def requires(self):
        return ValidateData(key=self.key)
