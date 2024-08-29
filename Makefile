SHELL:=/bin/bash
PROJECT=imio-luigi
VERSION=3.10.12
VENV=${PROJECT}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python
BIN_PATH ?=

init:
	pyenv virtualenv ${VERSION} ${VENV}
	echo ${VENV} > .python-version
	$(PYTHON) -m $(BIN_PATH)pip install install -r requirements.txt

update:
	$(PYTHON) -m $(BIN_PATH)pip install -r requirements.txt

# Dison
.PHONY: run-local-dison
run-local-dison:
	LUIGI_CONFIG_PATH=$(CURDIR)/dison.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_dison dison.GetFromAccess --counter="500" --filepath ./data/dison/AgoraWin/json/URBA.json --local-scheduler

# Faimes
.PHONY: run-local-faimes
run-local-faimes:
	LUIGI_CONFIG_PATH=$(CURDIR)/faimes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_faimes faimes.GetFromAccess --filepath ./data/faimes/AgoraWin/json/URBA_missingg_permis.json --local-scheduler

.PHONY: import-faimes
import-faimes:
	LUIGI_CONFIG_PATH=./faimes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-faimes --local-scheduler

# Flemalle
.PHONY: run-local-flemalle-notaries
run-local-flemalle-notaries:
	LUIGI_CONFIG_PATH=$(CURDIR)/flemalle.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_flemalle_notaire flemalle.GetFromCSV --filepath ./data/flemalle/File_V_rename.csv --local-scheduler

.PHONY: import-flemalle-notaries
import-flemalle-notaries:
	LUIGI_CONFIG_PATH=./flemalle.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-flemalle-notaries --local-scheduler --logging-conf-file logging_flemalle.ini

# Arlon
.PHONY: run-local-arlon
run-local-arlon:
	LUIGI_CONFIG_PATH=$(CURDIR)/arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_arlon arlon.GetFromAccess --filepath ./data/arlon/json/REGISTRES.json --local-scheduler

.PHONY: import-arlon
import-arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-arlon --local-scheduler

.PHONY: change_workflow_arlon
change_workflow_arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.change_workflow_json_arlon arlon.GetJSONFile --path ./result-arlon-preprocess --local-scheduler

.PHONY: fix_missing_usage_arlon
fix_missing_usage_arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.fix_usage_missing_json_arlon arlon.GetJSONFile --path ./result-arlon-preprocess --local-scheduler

# Berloz
.PHONY: run-local-berloz
run-local-berloz:
	LUIGI_CONFIG_PATH=$(CURDIR)/berloz.cfg $(BIN_PATH)luigi --count=10 --module imio_luigi.urban.export_berloz berloz.GetFromCSV --filepath ./data/berloz/donnees_traitees.csv --local-scheduler


# Mouscron
.PHONY: run-local-mouscron
run-local-mouscron:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_mouscron mouscron.GetFromCSV --filepath ./data/mouscron/p/p_permis_rename.csv --local-scheduler

.PHONY: run-local-mouscron-architecte
run-local-mouscron-architecte:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_mouscron_architect mouscron-architect.GetFromCSV --filepath ./data/mouscron/c/c_organisme.csv --local-scheduler

.PHONY: import-mouscron-architecte
import-mouscron-architecte:
	LUIGI_CONFIG_PATH=./mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-mouscron-architect --local-scheduler

.PHONY: import-mouscron
import-mouscron:
	LUIGI_CONFIG_PATH=./mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-mouscron --local-scheduler --logging-conf-file logging.ini

.PHONY: run-local-fix-ref-mouscron
run-local-fix-ref-mouscron:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.fix_ref_mouscron mouscron.GetJSONFile --path ./result-mouscron-preprocess --local-scheduler

# Ecaussinnes
.PHONY: run-local-ecaussinnes
run-local-ecaussinnes:
	LUIGI_CONFIG_PATH=$(CURDIR)/ecaussinnes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_ecaussinnes ecaussinnes.GetFromXML --filepath ./data/ecaussinnes/ --local-scheduler


.PHONY: import-ecaussinnes
import-ecaussinnes:
	LUIGI_CONFIG_PATH=./ecaussinnes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-ecaussinnes --local-scheduler

# Grace-Hollogne
.PHONY: run-local-gracehollogne
run-local-gracehollogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_gracehollogne gracehollogne.GetFromAccess --filepath ./data/gracehollogne/customer_ouput_formated.json --local-scheduler


.PHONY: clear-gracehollogne-env
clear-gracehollogne-env:
	rm -rf result-gracehollogne-env/ failures/gracehollogne_env-*

.PHONY: import-local-gracehollogne
import-gracehollogne:
	LUIGI_CONFIG_PATH=./gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-gracehollogne --local-scheduler

.PHONY: import-local-gracehollogne-env
import-gracehollogne-env:
	LUIGI_CONFIG_PATH=./gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-gracehollogne-env --local-scheduler --logging-conf-file logging_gracehollogne.ini

# Bastonge
.PHONY: run-local-bastogne
run-local-bastogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/bastogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb82003ac --local-scheduler

# OLLN
.PHONY: run-local-olln
run-local-olln:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --orga=olln --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb25121ac --local-scheduler

.PHONY: import-olln
import-olln:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-olln --local-scheduler

#Hensies
.PHONY: run-local-hensies
run-local-hensies:
	LUIGI_CONFIG_PATH=./hensies.cfg $(BIN_PATH)luigi --orga=hensies --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb53039ac --local-scheduler

.PHONY: import-hensies
import-hensies:
	LUIGI_CONFIG_PATH=./hensies.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-hensies --local-scheduler

# Acropole
.PHONY: run-local-acropole
run-local-acropole:
	luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --local-scheduler

.PHONY: import-local
import-local:
	LUIGI_CONFIG_PATH=./dison.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-dison --local-scheduler

test:
	nosetests imio_luigi

acropole-db:
	docker-compose -f docker-compose-acropole.yml -p acropole up

.PHONY: run-local-libreoffice-upgrade-templates
run-local-libreoffice-upgrade-templates:
	LUIGI_CONFIG_PATH=./config/local.cfg luigi --module imio_luigi.urban.libreoffice libreoffice.UpgradeTemplatesTask --local-scheduler
