# -*- coding: utf-8 -*-

DEFAULT_CONFIG = {
    "key":  "reference",
    "search_on": "getReference",
    "change_workflow": True
}

CONTACT_CONFIG = {
    "key":  "id",
    "search_on": "id",
    "change_workflow": False
}

workflows = {
    "codt_buildlicence_workflow" : {
        "default_transition": "deposit",
        "transition" : {
            "deposit": [],
            "accepted" : ["iscomplete", "accept"],
            "inacceptable": ["isinacceptable"],
            "incomplete" : ["isincomplete"],
            "complete": ["iscomplete"],
            "refused": ["iscomplete", "refuse"],
            "retired": ["iscomplete", "retire"]
        },
        "mapping" : {}
    },
    "env_licence_workflow" : {
        "default_transition": "deposit",
        "transition" : {
            "deposit": [],
            "accepted" : [
                "iscomplete",
                'prepare_college_opinion',
                "wait_FT_opinion",
                "prepare_final_decision",
                "accept"
            ],
            "inacceptable": ["isinacceptable"],
            "incomplete" : ["isincomplete"],
            "complete": ["iscomplete"],
            "refused": [
                "iscomplete",
                'prepare_college_opinion',
                "wait_FT_opinion",
                "prepare_final_decision",
                "refuse"
            ],
            "retired": [
                "iscomplete",
                'prepare_college_opinion',
                "wait_FT_opinion",
                "prepare_final_decision",
                "retire"
            ],
            "college_opinion": [
                "iscomplete",
                'prepare_college_opinion',
            ],
            "FT_opinion": [
                "iscomplete",
                'prepare_college_opinion',
                "wait_FT_opinion",
            ],
            "final_decision_in_progress": [
                "iscomplete",
                'prepare_college_opinion',
                "wait_FT_opinion",
                "prepare_final_decision",
            ]
        },
        "mapping" : {}
    },
    "inspection_workflow" : {
        "default_transition": "creation",
        "transition" : {
            "creation": [],
            "analysis": ["analyse"],
            "administrative_answer": ["analyse","give_answer"],
            "ended": ["analyse","give_answer","close"],
        },
        "mapping" : {
            "deposit": "creation",
            "accepted": "creation",
            "inacceptable": "creation",
            "incomplete": "creation",
            "complete": "creation",
            "refused": "creation",
            "retired": "creation",
        }
       
    },
    "ticket_workflow" : {
        "default_transition": "creation",
        "transition" : {
            "creation": [],
            "prosecution_analysis": ["send_to_prosecutor"],
            "in_progress_with_prosecutor": ["send_to_prosecutor","follow_with_prosecutor"],
            "in_progress_without_prosecutor": ["send_to_prosecutor", "follow_without_prosecutor"],
            "ended": ["send_to_prosecutor","follow_with_prosecutor","close"],
        },
        "mapping" : {
            "deposit": "creation",
            "accepted": "creation",
            "inacceptable": "creation",
            "incomplete": "creation",
            "complete": "creation",
            "refused": "creation",
            "retired": "creation",
        }
    },
    "urbandivision_workflow" : {
        "default_transition": "in_progress",
        "transition" : {
            "in_progress": [],
            "accepted": ["accept"],
            "need_parceloutlicence": ["nonapplicable"],
        },
        "mapping" : {
            "deposit" : "in_progress",
            "inacceptable": "need_parceloutlicence",
            "incomplete": "in_progress",
            "complete": "in_progress",
            "refused": "need_parceloutlicence",
            "retired": "need_parceloutlicence",
        }
    },
    "urban_licence_workflow" : {
        "default_transition": "in_progress",
        "transition" : {
            "in_progress": [],
            "accepted": ["accept"],
            "incomplete": ["isincomplete"],
            "refused": ["refuse"],
            "retired": ["retire"],
        },
        "mapping" : {
            "deposit" : "in_progress",
            "inacceptable": "refused",
            "complete": "in_progress"
        }
    }
}

config = {
    "BuildLicence": {
        "folder": "buildlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_BuildLicence": {
        "folder": "codt_buildlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "Article127": {
        "folder": "article127s",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_Article127": {
        "folder": "codt_article127s",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "IntegratedLicence": {
        "folder": "integratedlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_IntegratedLicence": {
        "folder": "codt_integratedlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "UniqueLicence": {
        "folder": "uniquelicences",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_UniqueLicence": {
        "folder": "codt_uniquelicences",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "Declaration": {
        "folder": "declarations",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "UrbanCertificateOne": {
        "folder": "urbancertificateones",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_UrbanCertificateOne": {
        "folder": "codt_urbancertificateones",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "UrbanCertificateTwo": {
        "folder": "urbancertificatetwos",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_UrbanCertificateTwo": {
        "folder": "codt_urbancertificatetwos",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "PreliminaryNotice": {
        "folder": "preliminarynotices",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "EnvClassOne": {
        "folder": "envclassones",
        "config": DEFAULT_CONFIG,
        "workflow": "env_licence_workflow",
    },
    "EnvClassTwo": {
        "folder": "envclasstwos",
        "config": DEFAULT_CONFIG,
        "workflow": "env_licence_workflow",
    },
    "EnvClassThree": {
        "folder": "envclassthrees",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "ParcelOutLicence": {
        "folder": "parceloutlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_ParcelOutLicence": {
        "folder": "codt_parceloutlicences",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "MiscDemand": {
        "folder": "miscdemands",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "NotaryLetter": {
        "folder": "notaryletters",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_NotaryLetter": {
        "folder": "codt_notaryletters",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "Inspection": {
        "folder": "inspections",
        "config": DEFAULT_CONFIG,
        "workflow": "inspection_workflow",
    },
    "Ticket": {
        "folder": "tickets",
        "config": DEFAULT_CONFIG,
        "workflow": "ticket_workflow",
    },
    "Architect": {
        "folder": "architects",
        "config": CONTACT_CONFIG,
    },
    "Geometrician": {
        "folder": "geometricians",
        "config": CONTACT_CONFIG,
    },
    "Notary": {
        "folder": "notaries",
        "config": CONTACT_CONFIG,
    },
    "Parcelling": {
        "folder": "parcellings",
        "config": CONTACT_CONFIG,
    },
    "Division": {
        "folder": "divisions",
        "config": DEFAULT_CONFIG,
        "workflow": "urbandivision_workflow",
    },
    "ProjectMeeting": {
        "folder": "projectmeeting",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "CODT_CommercialLicence": {
        "folder": "codt_commerciallicences",
        "config": DEFAULT_CONFIG,
        "workflow": "codt_buildlicence_workflow",
    },
    "ExplosivesPossession": {
        "folder":"ExplosivesPossession",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    },
    "PatrimonyCertificate": {
        "folder": "patrimonycertificates",
        "config": DEFAULT_CONFIG,
        "workflow": "urban_licence_workflow",
    }
}