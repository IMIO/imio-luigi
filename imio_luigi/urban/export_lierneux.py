# -*- coding: utf-8 -*-

from datetime import datetime
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
            # if row["REF"] not in ["2000/U047"]:
            #     continue
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
    log_failure = False
    config_dict = {
        "encours": {
            "file": "./data/lierneux/json/Urbi7/encours.json",
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

    def requires(self):
        return ValueCleanup(key=self.key)

class Mapping(core.MappingKeysInMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    mapping = {
        "REF": "reference",
        "TYPE": "@type",
        "CADASTRE": "cadastre",
        "OBJET": "licenceSubject",
        "urba": "referenceDGATLP",
        "refcloture": "additionalReference"
    }

    def requires(self):
        return AddRelation(key=self.key)


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
        if data.get("refuse") in [0, 1]:
            decision = "favorable"
        elif data.get("refuse") == -1:
            decision = "defavorable"
        else:
            decision = None
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
                "CODT_BuildLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_CommercialLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_ParcelOutLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_UrbanCertificateOne": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "CODT_UrbanCertificateTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "EnvClassTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "MiscDemand": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "UniqueLicence": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateOne": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateTwo": {"urban_type": "debut-des-travaux", "date": ["eventDate"]},
            }
        },
        "end_work": {
            "check_key": ["TravauxFin"],
            "date_mapping": {"eventDate": "TravauxFin"},
            "mapping": {
                "Article127": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "BuildLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_BuildLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_CommercialLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_ParcelOutLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_UrbanCertificateOne": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "CODT_UrbanCertificateTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "EnvClassTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "MiscDemand": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "UniqueLicence": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateOne": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
                "UrbanCertificateTwo": {"urban_type": "fin-des-travaux", "date": ["eventDate"]},
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
        if data.get("refuse") in [0, 1]:
            state = "accepted"
        elif data.get("refuse") == -1:
            state = "refused"
        if state is not None:
            data["wf_transitions"] = [state]
        return data


class UrbanTransitionMapping(ucore.UrbanTransitionMapping):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def requires(self):
        return AddTransitions(key=self.key)


class CreateApplicant(core.CreateSubElementInMemoryTask):
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

    def modify_value(self, value):
        if isinstance(value, str):
            value = value.strip()
        return value

    def apply_subelement_base_type(self, data):
        self.subelement_base["@type"] = ucore.config[data["@type"]]["contact_type"]

    def check_if_applicant(self, data):
        property = data.get("PROPRIET", None)
        if property is None or property == "":
            return False
        return True

    def transform_data(self, data):
        if self.check_if_applicant(data):
            self.apply_subelement_base_type(data)
            data = super().transform_data(data)
        return data

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
    known_villages = {
        "odrimont",
        "villettes",
        "jevigné",
        "trou de bra",
        "les villettes",
        "arbrefontaine",
        "bra",
        "lierneux",
        "4990 lierneux",
        "lansival"
    }

    def safe_convert_string_to_int(self, string):
        try:
            return int(string)
        except ValueError:
            return string

    def is_village(self, token: str) -> bool:
        return token.strip().lower() in self.known_villages

    def split_street_number(self, worklocation):
        s = worklocation.strip()
        s = re.sub(r'\s*-\s*$', '', s)
        s = re.sub(r'\s{2,}', ' ', s)
        s = re.sub(r',\s*$', '', s)
        s = re.sub(r'\s*\+\s*$', '', s)
        s = re.sub(r'^\d{4}\s+[A-Z]+,?\s*', '', s)
        s = re.sub(r'\s*\(lieu-dit', ', lieu-dit', s, flags=re.IGNORECASE)
        s = re.sub(r'\s*\+\s*$', '', s)
        s = re.sub(r',?\s*-?\s*\d{4}\s+[A-Z]+\s*$', '', s)
        s = re.sub(r'\bSt-Jacques\b', 'Saint-Jacques', s, flags=re.IGNORECASE)
        s = re.sub(r'\bRte\b', 'Route', s, flags=re.IGNORECASE)
        s = re.sub(r'^R\s+(?=[A-Z])', 'Rue ', s)

        NUMBER_RE = r'(?:s/n°|S/N|s/n|n°\s*\d+\s?[a-zA-Z]*(?:\s*/\s*[a-zA-Z\d]+)?(?:-\d+[a-zA-Z]*)*\+?|\d+\s?[a-zA-Z]*(?:\s*/\s*[a-zA-Z\d]+)?(?:-\d+[a-zA-Z]*)*\+?)'
        LIEU_DIT_RE = r'(?:(?:au|en)\s+)?(?:lieux?-dits?|lieux?\s+dits?|lieudits?)\s+(?:["\u201c]([^"\u201d]*)["\u201d]|\S+)'
        ROUTE_RE = r'[Rr]oute\s+de\s+[^,]+'

        result = {"town": None, "street": None, "number": None, "note": None}

        lieu_dit_match = re.search(LIEU_DIT_RE, s, flags=re.IGNORECASE)
        if lieu_dit_match:
            quoted = lieu_dit_match.group(1)
            result["note"] = lieu_dit_match.group(0).strip()
            before = s[:lieu_dit_match.start()].strip().rstrip(',').strip()

            num_before = re.search(rf',?\s*({NUMBER_RE})\s*$', before, flags=re.IGNORECASE)
            if num_before:
                result["number"] = num_before.group(1)
                before = before[:num_before.start()].strip().rstrip(',').strip()

            parts_before = [p.strip() for p in before.split(',') if p.strip()]

            if len(parts_before) == 0:
                result["street"] = quoted
            elif len(parts_before) == 1:
                token = parts_before[0]
                if self.is_village(token) or ' ' not in token and not re.match(
                    r'^(?:Rue|Route|Avenue|Chemin)\b', token, re.IGNORECASE
                ):
                    result["town"] = token
                    result["street"] = quoted
                else:
                    result["street"] = token
            elif len(parts_before) >= 2:
                result["town"] = parts_before[0]
                result["street"] = parts_before[1]

            s = ""

        elif re.search(ROUTE_RE, s, flags=re.IGNORECASE):
            route_match = re.search(ROUTE_RE, s, flags=re.IGNORECASE)
            result["street"] = route_match.group(0).strip()
            after = s[route_match.end():].strip()
            num_after = re.search(rf',?\s*({NUMBER_RE})\s*$', after, flags=re.IGNORECASE)
            if num_after:
                result["number"] = num_after.group(1)
            s = s[:route_match.start()].strip().rstrip(',').strip()

        elif re.search(r'["\u201c]([^"\u201d]+)["\u201d]', s):
            quoted_match = re.search(r'["\u201c]([^"\u201d]+)["\u201d]', s)
            result["note"] = quoted_match.group(0).strip()
            s = s[:quoted_match.start()].strip().rstrip(',').strip()
            if not s:
                result["street"] = quoted_match.group(1).strip()

        if s:
            num_match = re.search(rf',?\s*\+?\s*({NUMBER_RE})\s*$', s, flags=re.IGNORECASE)
            if num_match:
                result["number"] = num_match.group(1)
                s = s[:num_match.start()].strip().rstrip(',').strip()
            else:
                num_match2 = re.search(rf'\s*\+?\s*({NUMBER_RE})\s*$', s, flags=re.IGNORECASE)
                if num_match2:
                    result["number"] = num_match2.group(1)
                    s = s[:num_match2.start()].strip()

        if result["number"]:
            result["number"] = re.sub(r'^n°\s*', '', result["number"], flags=re.IGNORECASE)

        if s:
            parts = [p.strip() for p in s.split(',') if p.strip()]

            if len(parts) == 0:
                pass
            elif len(parts) == 1:
                token = parts[0]
                if result["street"] is not None:
                    result["town"] = token
                elif self.is_village(token):
                    result["town"] = token
                elif ' ' not in token or result["number"] is not None or re.match(
                    r'^(?:Rue|Route|Avenue|Chemin)\b', token, re.IGNORECASE
                ):
                    result["street"] = token
                else:
                    result["town"] = token
            elif len(parts) == 2:
                result["town"] = parts[0]
                if result["street"] is None:
                    result["street"] = parts[1]
            elif len(parts) >= 3:
                result["town"] = parts[0]
                if result["street"] is None:
                    result["street"] = ', '.join(parts[1:])

        with open("export_street.txt", "a") as f:
            f.write(f"{worklocation.strip()} : {result['town']} | {result['street']} | {result['number']} | {result['note']} \n")

        result["street"] = result["street"] or ""
        result["number"] = result["number"] or ""
        del result["note"]
        del result["town"]
        if not result["street"]:
            return None
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
        if new_element is not None:
            data[self.subelements_destination_key].append(new_element)
        return data

    def requires(self):
        return CreateApplicant(key=self.key)


class TransformWorkLocation(ucore.TransformWorkLocationMultiParams):
    task_namespace = "lierneux"
    key = luigi.Parameter()
    log_failure = True

    def requires(self):
        return CreateWorkLocation(key=self.key)

    def _generate_params(self, worklocation, data):
        street = worklocation.get("street", None)
        town = worklocation.get("town", None)
        if street is None:
            return None, None
        params = [
            {
                "key": "term",
                "value": street
            }]
        if street is not None and town is not None:
            params.append({
                "key": "term",
                "value": f"{street} {town}"
            })
        return params, None

    def get_range_number(self, number):
        number_list = number.split("-")
        if len(number_list) == 2:
            try:
                return list(range(int(number_list[0]), int(number_list[1]) + 1))
            except ValueError:
                return number_list
        elif len(number_list) > 2:
            return number_list
        return number_list

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
        numbers = self.get_range_number(number)
        data["workLocations"] = [
            {
                "street": worklocation["street"],
                "number": str(n)
            }
            for n in numbers
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


class CadastreSplit(core.InMemoryTask):
    task_namespace = "lierneux"
    key = luigi.Parameter()

    def split_cadastres(self, cadastre_str):
        cleaned = re.sub(r'^[nN]\s*°?\s*', '', cadastre_str.strip())
        parts = re.split(r'\s+et\s+|\s*,\s*|\s*\.\.\.\s*', cleaned)
        return [p.strip() for p in parts if p.strip() and re.match(r'^\d', p.strip())]

    def transform_data(self, data):
        if "cadastre" not in data:
            return data
        data["cadastre"] = self.split_cadastres(data["cadastre"])
        return data

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

    def extract_cadastre(self, cadastre):
        pattern = r"^(?P<radical>\d+)(?P<exposant>[a-zA-Z]?)(?P<puissance>\d?)(?:/(?P<bis>\w+))?$"
        match = re.match(pattern, cadastre.strip())
        if not match:
            return None
        result = match.groupdict()
        return {k: (v or '') for k, v in result.items()}

    def build_search_params(self, division, section, parcel):
        extracted = self.extract_cadastre(parcel)
        if not extracted:
            return None, f"No parcel extract for {parcel}"
        params = {
            'division': division,
            'section': section.upper(),
            **extracted
        }
        return self._fix_lower_chars(params), None

    def _generate_cadastre_dict(self, cadastre, data):
        return self.build_search_params(
            self._mapping_division(data["division"]),
            data["section"],
            cadastre
        )


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
        "NumPermis",
        "AdresseBien",
        "RefLotDate",
        "RefLotNo",
        "RefBatiDate",
        "RefBatiNo",
        "EcheanceAction",
        "EcheanceDate",
        "EcheanceStatus",
        "Auteur_projet",
        "AvisFonctionnaire",
        "ARCHI",
        "ArchiBureau",
        "REF_CAD",
        "CadastreType",
        "MANDANT",
        "DGATLP",
        "encours",
    ]
    key_dict = {
        "AdresseBien": "Adresse du bien",
        "EcheanceAction": "Echeance action",
        "EcheanceDate": "Echeance date",
        "EcheanceStatus": "Echeance status",
        "Auteur_projet": "Auteur du projet",
        "AvisFonctionnaire": "Avis du fonctionnaire",
        "encours": "Encours",
        "DGATLP": "DGATLP",
        "ARCHI": "Architecte",
        "CadastreType": "Type de cadastre",
        "MANDANT": "Demandant",
        "RefLotDate": "Ref Lot Date",
        "RefLotNo": "Ref Lot No",
        "ArchiBureau": "Bureau Archi",
        "RefBatiNo": "Ref Bati No",
        "NumPermis": "Num Permis",
        "RefBatiDate": "Ref Bati Date",
        "REF_CAD": "REF CAD"
    }
    keys_date = [
        "EcheanceDate",
        "RefLotDate",
        "RefBatiDate"
    ]
    new_line = "\n"

    def fix_key(self, key):
        return self.key_dict.get(key, key)

    def get_values(self, data):
        result = []
        for key in self.key_to_description:
            value = data.get(key, None)
            if value is not None:
                result.append(
                    {"key": key, "value": data[key]}
                )
        return result

    def handle_value(self, value, data):
        key = value["key"]
        value = value["value"]
        if key == "encours":
            value = self.handle_encours(value)
        if key == "DGATLP":
            value = self.handle_DGATLP(value)
        data["description"]["data"] += f"{self.fix_key(key)} : {value}"
        return data

    def pretify_date(self, date):
        if date is None:
            return None
        try:
            iso_date = datetime.strptime(date, "%m/%d/%y %H:%M:%S")
            export_date = iso_date.strftime("%d/%m/%Y")
        except Exception:
            export_date = f"Erreur avec la date : {date}"

        return export_date

    def handle_encours(self, value):
        events = []
        for event in value:
            event_title = "Évènement sans titre"
            action = event.get("actions", None)
            if action is not None:
                event_title = action.get("COMMENT", event_title)
            output = {}
            output["Document"] = event.get("DocumentName", None)
            output["Commentaire de réception"] = event.get("comment_reception", None)
            output["Date de réception"] = self.pretify_date(event.get("réception", None))
            output["Commentaire d'envoi"] = event.get("comment_envoi", None)
            output["Date d'envoi"] = self.pretify_date(event.get("envoi", None))
            output["Date d'échéance"] = self.pretify_date(event.get("echeance", None))
            output["Avis"] = event.get("avis", None)
            data = []
            for title, content in output.items():
                if content is None or content == "":
                    continue
                data.append(f"<li>{title}: {content}</li>")
            new_line = "\n"
            events.append(
                f"<li>{event_title} : <ul>{new_line.join(data)}</ul></li>"
            )
        return f"<ul>{self.new_line.join(events)}</ul>"

    def pretify_camel_string(self, string):
        return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", string)

    def handle_DGATLP(self, value):
        output = []
        keys = [
            "Eau",
            "TypeRevetement",
            "Revetement",
            "EpurationIndividuelle",
            "Publicite",
            "Egout",
            "OuvertureVoirie",
            "Lotissement",
            "ZonePS",
            "Dossiers",
            "Electricite",
            "REF",
            "ResponsableVoirie",
            "Reclamations"
        ]
        for key in keys:
            if key in value and value[key] is not None:
                output.append(f"<li>{self.pretify_camel_string(key)}: {value[key]}</li>")
        value = f"<ul>{self.new_line.join(output)}</ul>"
        return value

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
        'Auteur_projet',
        "ArchiBureau",
        "DGATLP",
        "encours",
        "note",
        "town"
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
