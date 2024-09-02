# -*- coding: utf-8 -*-

DEFAULT_CONFIG = {
    "key": "reference",
    "search_on": "getReference",
    "change_workflow": True,
}

CONTACT_CONFIG = {"key": "id", "search_on": "id", "change_workflow": False}

workflows = {
    "codt_buildlicence_workflow": {
        "default_transition": "deposit",
        "transition": {
            "deposit": [],
            "accepted": ["iscomplete", "accept"],
            "inacceptable": ["isinacceptable"],
            "incomplete": ["isincomplete"],
            "complete": ["iscomplete"],
            "refused": ["iscomplete", "refuse"],
            "retired": ["iscomplete", "retire"],
        },
        "mapping": {},
    },
    "env_licence_workflow": {
        "default_transition": "deposit",
        "transition": {
            "deposit": [],
            "accepted": [
                "iscomplete",
                "prepare_college_opinion",
                "wait_FT_opinion",
                "prepare_final_decision",
                "accept",
            ],
            "inacceptable": ["isinacceptable"],
            "incomplete": ["isincomplete"],
            "complete": ["iscomplete"],
            "refused": [
                "iscomplete",
                "prepare_college_opinion",
                "wait_FT_opinion",
                "prepare_final_decision",
                "refuse",
            ],
            "retired": [
                "iscomplete",
                "prepare_college_opinion",
                "wait_FT_opinion",
                "prepare_final_decision",
                "retire",
            ],
            "college_opinion": [
                "iscomplete",
                "prepare_college_opinion",
            ],
            "FT_opinion": [
                "iscomplete",
                "prepare_college_opinion",
                "wait_FT_opinion",
            ],
            "final_decision_in_progress": [
                "iscomplete",
                "prepare_college_opinion",
                "wait_FT_opinion",
                "prepare_final_decision",
            ],
        },
        "mapping": {},
    },
    "inspection_workflow": {
        "default_transition": "creation",
        "transition": {
            "creation": [],
            "analysis": ["analyse"],
            "administrative_answer": ["analyse", "give_answer"],
            "ended": ["analyse", "give_answer", "close"],
        },
        "mapping": {
            "deposit": "creation",
            "accepted": "creation",
            "inacceptable": "creation",
            "incomplete": "creation",
            "complete": "creation",
            "refused": "creation",
            "retired": "creation",
        },
    },
    "ticket_workflow": {
        "default_transition": "creation",
        "transition": {
            "creation": [],
            "prosecution_analysis": ["send_to_prosecutor"],
            "in_progress_with_prosecutor": [
                "send_to_prosecutor",
                "follow_with_prosecutor",
            ],
            "in_progress_without_prosecutor": [
                "send_to_prosecutor",
                "follow_without_prosecutor",
            ],
            "ended": ["send_to_prosecutor", "follow_with_prosecutor", "close"],
        },
        "mapping": {
            "deposit": "creation",
            "accepted": "creation",
            "inacceptable": "creation",
            "incomplete": "creation",
            "complete": "creation",
            "refused": "creation",
            "retired": "creation",
        },
    },
    "urbandivision_workflow": {
        "default_transition": "in_progress",
        "transition": {
            "in_progress": [],
            "accepted": ["accept"],
            "need_parceloutlicence": ["nonapplicable"],
        },
        "mapping": {
            "deposit": "in_progress",
            "inacceptable": "need_parceloutlicence",
            "incomplete": "in_progress",
            "complete": "in_progress",
            "refused": "need_parceloutlicence",
            "retired": "need_parceloutlicence",
        },
    },
    "urban_licence_workflow": {
        "default_transition": "in_progress",
        "transition": {
            "in_progress": [],
            "accepted": ["accept"],
            "incomplete": ["isincomplete"],
            "refused": ["refuse"],
            "retired": ["retire"],
        },
        "mapping": {
            "deposit": "in_progress",
            "inacceptable": "refused",
            "complete": "in_progress",
        },
    },
}

config = {
    "BuildLicence": {
        "folder": "buildlicences",
        "config_folder": "buildlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_BuildLicence": {
        "folder": "codt_buildlicences",
        "config_folder": "codt_buildlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "Article127": {
        "folder": "article127s",
        "config_folder": "article127",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_Article127": {
        "folder": "codt_article127s",
        "config_folder": "codt_article127",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "IntegratedLicence": {
        "folder": "integratedlicences",
        "config_folder": "integratedlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_IntegratedLicence": {
        "folder": "codt_integratedlicences",
        "config_folder": "codt_integratedlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "UniqueLicence": {
        "folder": "uniquelicences",
        "config_folder": "uniquelicence",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_UniqueLicence": {
        "folder": "codt_uniquelicences",
        "config_folder": "codt_uniquelicence",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "Declaration": {
        "folder": "declarations",
        "config_folder": "declaration",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "UrbanCertificateOne": {
        "folder": "urbancertificateones",
        "config_folder": "urbancertificateone",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_UrbanCertificateOne": {
        "folder": "codt_urbancertificateones",
        "config_folder": "codt_urbancertificateone",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "UrbanCertificateTwo": {
        "folder": "urbancertificatetwos",
        "config_folder": "urbancertificatetwo",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_UrbanCertificateTwo": {
        "folder": "codt_urbancertificatetwos",
        "config_folder": "codt_urbancertificatetwo",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "PreliminaryNotice": {
        "folder": "preliminarynotices",
        "config_folder": "preliminarynotice",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "EnvClassOne": {
        "folder": "envclassones",
        "config_folder": "envclassone",
        "config": DEFAULT_CONFIG,
        "workflow": "env_licence_workflow",
        'contact_type': 'Applicant'
    },
    "EnvClassTwo": {
        "folder": "envclasstwos",
        "config_folder": "envclasstwo",
        "config": DEFAULT_CONFIG,
        "workflow": "env_licence_workflow",
        'contact_type': 'Applicant'
    },
    "EnvClassThree": {
        "folder": "envclassthrees",
        "config_folder": "envclassthree",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "ParcelOutLicence": {
        "folder": "parceloutlicences",
        "config_folder": "parceloutlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_ParcelOutLicence": {
        "folder": "codt_parceloutlicences",
        "config_folder": "codt_parceloutlicence",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "MiscDemand": {
        "folder": "miscdemands",
        "config_folder": "miscdemand",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "NotaryLetter": {
        "folder": "notaryletters",
        "config_folder": "notaryletter",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_NotaryLetter": {
        "folder": "codt_notaryletters",
        "config_folder": "codt_notaryletter",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "Inspection": {
        "folder": "inspections",
        "config_folder": "inspection",
        "config": DEFAULT_CONFIG,
        "workflow": "inspection_workflow",
        'contact_type': 'Applicant'
    },
    "Ticket": {
        "folder": "tickets",
        "config_folder": "ticket",
        "config": DEFAULT_CONFIG,
        "workflow": "ticket_workflow",
        'contact_type': 'Applicant'
    },
    "Architect": {
        "folder": "architects",
        "config_folder": "architect",
        "config": CONTACT_CONFIG,
    },
    "Geometrician": {
        "folder": "geometricians",
        "config_folder": "geometrician",
        "config": CONTACT_CONFIG,
    },
    "Notary": {
        "folder": "notaries",
        "config_folder": "notarie",
        "config": CONTACT_CONFIG,
    },
    "Parcelling": {
        "folder": "parcellings",
        "config_folder": "parcelling",
        "config": CONTACT_CONFIG,
    },
    "Division": {
        "folder": "divisions",
        "config_folder": "division",
        "config": DEFAULT_CONFIG,
        "workflow": "urbandivision_workflow",
        'contact_type': 'Proprietary'
    },
    "ProjectMeeting": {
        "folder": "projectmeetings",
        "config_folder": "projectmeeting",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "CODT_CommercialLicence": {
        "folder": "codt_commerciallicences",
        "config_folder": "codt_commerciallicence",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
        'contact_type': 'Applicant'
    },
    "ExplosivesPossession": {
        "folder": "ExplosivesPossession",
        "config_folder": "explosivespossession",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
    "PatrimonyCertificate": {
        "folder": "patrimonycertificates",
        "config_folder": "patrimonycertificate",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
        'contact_type': 'Applicant'
    },
}
