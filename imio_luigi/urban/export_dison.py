# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban.address import find_address_match

import json
import logging
import luigi


logger = logging.getLogger("luigi-interface")

"""
- "Numero": Référence du permis
- "LibNat": L'objet
- "Rec": Type de permis, liste des valeurs dans CHOIX-PERMIS.json
- "C_Adres": Rue (ex: "Avenue Jardin Ecole")
- "C_Code": Code postal (ex: 4820)
- "C_Loc": Localité (ex: "Dison")
- "C_Num": Numéro (ex: "44")
- "Cadastre": Parcelles (ex: "602 X 6", "278k2,278r 280b4", "991 e6, 991s3, 998c parties", "715d, 717b, 715e", "594 R6 ET 59 9 s") Note : 602 X 6 : 602 Radical, X exposant, 6 puissance
- "D_Adres": Adresse demandeur (ex: "rue Spintay 166")
- "D_Code": Code postal demandeur (ex: 4800)
- "D_Tel": Téléphone demandeur
- "D_GSM": GSM demandeur (ex: "0496/963196")
- "D_Loc": Localité demandeur (ex: "Verviers")
- "D_Nom": Nom demandeur (ex: "Stini")
- "D_Prenom": Prénom demandeur (ex: "Samira")
- "D_Pays": Pays du demandeur
- "UR_Avis": Avis urbanisme ???, liste des valeurs dans AVIS.json
- "MT": Avis collège ???, liste des valeurs dans Avis-College.json
- "A_Nom": Nom de l'architecte
- "A_Prenom": Prenom de l'architecte
"""


class GetFromAccess(core.GetFromAccessJSONTask):
    task_namespace = "dison"
    filepath = luigi.Parameter()
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
    ]

    def run(self):
        for row in self.query():
            yield Transform(key=row["Numero"], data=row)


class Transform(luigi.Task):
    task_namespace = "dison"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            f.write(json.dumps(dict(self.data)))
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


class AddExtraData(core.AddDataInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    filepath = "./add-data-dison.json"

    def requires(self):
        return Mapping(key=self.key)


class MappingCountry(utils.MappingCountryInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping_key = "D_Pays"

    def requires(self):
        return AddExtraData(key=self.key)


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

    def requires(self):
        return MappingCountry(key=self.key)


class WorkLocationSplit(core.StringToListInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    attribute_key = "C_Adres"
    separators = [",", " ET ", " et "]

    def requires(self):
        return CreateApplicant(key=self.key)


class CreateWorkLocation(core.CreateSubElementsFromSubElementsInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    subelements_source_key = "C_Adres"
    subelements_destination_key = "workLocations"
    mapping_keys = {}
    subelement_base = {}

    def transform_data(self, data):
        if self.subelements_destination_key not in data:
            if self.create_container is False:
                raise KeyError(f"Missing key {self.subelements_destination_key}")
            data[self.subelements_destination_key] = []
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

    def requires(self):
        return CreateWorkLocation(key=self.key)

    @property
    def request_url(self):
        return f"{self.url}/@address"

    def transform_data(self, data):
        new_work_locations = []
        for worklocation in data["workLocations"]:
            param_values = [
                str(v)
                for k, v in worklocation.items()
                if v and k in ("street", "locality", "zip")
            ]
            params = {"term": " ".join(param_values)}
            r = self.request(parameters=params)
            if r.status_code != 200:
                raise ValueError(f"Response code is '{r.status_code}', expected 200")
            result = r.json()
            if result["items_total"] == 0:
                raise ValueError(
                    f"No result for location search on query '{params['term']}'"
                )
            elif result["items_total"] > 1:
                match = find_address_match(result["items"], worklocation["street"])
                if not match:
                    raise ValueError(
                        f"Multiple results for location search on query '{params['term']}'"
                    )
            else:
                match = result["items"][0]
            new_work_locations.append(
                {
                    "street": match["uid"],
                    "number": worklocation.get("number", ""),
                }
            )
        data["workLocations"] = new_work_locations
        return data


class MappingType(core.MappingValueWithFileInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    mapping_filepath = "./mapping-type-dison.json"
    mapping_key = "@type"

    def requires(self):
        return TransformWorkLocation(key=self.key)


class CadastreSplit(core.StringToListInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    attribute_key = "cadastre"
    separators = [",", " ET "]

    def requires(self):
        return MappingType(key=self.key)


class DropColumns(core.DropColumnInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    drop_keys = [
        "Section",
        "C_Loc",
        "C_Code",
        "C_Adres",
        "C_Num",
        "cadastre",  # temporary
        "UR_Avis",  # temporary
        "A_Nom",  # temporary
        "A_Prenom",  # temporary
        "MT",  # temporary
    ]

    def requires(self):
        return CadastreSplit(key=self.key)


class ValidateData(core.JSONSchemaValidationTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    schema_path = "./imio_luigi/urban/schema/licence.json"

    def requires(self):
        return DropColumns(key=self.key)


class UpdateReference(core.UpdateReferenceInMemoryTask):
    task_namespace = "dison"
    key = luigi.Parameter()
    rules_filepath = "./reference-dison.json"

    def requires(self):
        return ValidateData(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "dison"
    export_filepath = "./result-dison"
    key = luigi.Parameter()

    def requires(self):
        return UpdateReference(key=self.key)
