SHELL:=/bin/bash
PROJECT=imio-luigi
VERSION=3.10.12
VENV=${PROJECT}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python
LUIGI_PATH ?=

init:
	pyenv virtualenv ${VERSION} ${VENV}
	echo ${VENV} > .python-version
	$(PYTHON) -m pip install install -r requirements.txt

update:
	$(PYTHON) -m pip install -r requirements.txt

# Dison
.PHONY: run-local-dison
run-local-dison:
	LUIGI_CONFIG_PATH=$(CURDIR)/dison.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_dison dison.GetFromAccess --counter="500" --filepath ./data/dison/AgoraWin/json/URBA.json --local-scheduler

# Faimes
.PHONY: run-local-dison
run-local-faimes:
	LUIGI_CONFIG_PATH=$(CURDIR)/faimes.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_faimes faimes.GetFromAccess --filepath ./data/faimes/AgoraWin/json/URBA.json --local-scheduler


# Arlon
.PHONY: run-local-arlon
run-local-arlon:
	LUIGI_CONFIG_PATH=$(CURDIR)/arlon.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_arlon arlon.GetFromAccess --filepath ./data/arlon/json/REGISTRES.json --local-scheduler

.PHONY: import-arlon
import-arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-arlon --local-scheduler

.PHONY: change_workflow_arlon
change_workflow_arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.change_workflow_json_arlon arlon.GetJSONFile --path ./result-arlon-preprocess --local-scheduler

.PHONY: fix_missing_usage_arlon
fix_missing_usage_arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.fix_usage_missing_json_arlon arlon.GetJSONFile --path ./result-arlon-preprocess --local-scheduler

# Berloz
.PHONY: run-local-berloz
run-local-berloz:
	LUIGI_CONFIG_PATH=$(CURDIR)/berloz.cfg $(LUIGI_PATH)luigi --count=10 --module imio_luigi.urban.export_berloz_a berloz.GetFromCSV --filepath ./data/berloz/Berloz-LISTE_DOSSIERS_jusque_1100.csv --local-scheduler


# Mouscron
.PHONY: run-local-mouscron
run-local-mouscron:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_mouscron mouscron.GetFromCSV --filepath ./data/mouscron/p/p_permis.csv --local-scheduler

.PHONY: run-local-mouscron-architecte
run-local-mouscron-architecte:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_mouscron_architect mouscron-architect.GetFromCSV --filepath ./data/mouscron/c/c_organisme.csv --local-scheduler


.PHONY: import-mouscron-architecte
import-mouscron-architecte:
	LUIGI_CONFIG_PATH=./mouscron.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-mouscron-architect --local-scheduler

# Ecaussinnes
.PHONY: run-local-ecaussinnes
run-local-ecaussinnes:
	LUIGI_CONFIG_PATH=$(CURDIR)/ecaussinnes.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_ecaussinnes ecaussinnes.GetFromXML --filepath ./data/ecaussinnes/ --local-scheduler


.PHONY: import-ecaussinnes
import-ecaussinnes:
	LUIGI_CONFIG_PATH=./ecaussinnes.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-ecaussinnes --local-scheduler

# Grace-Hollogne
.PHONY: run-local-gracehollogne
run-local-gracehollogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/gracehollogne.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_gracehollogne gracehollogne.GetFromAccess --filepath ./data/gracehollogne/customer_ouput_formated.json --local-scheduler


.PHONY: import-local-gracehollogne
import-gracehollogne:
	LUIGI_CONFIG_PATH=./gracehollogne.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-gracehollogne --local-scheduler

# Bastonge
.PHONY: run-local-bastogne
run-local-bastogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/bastogne.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb82003ac --local-scheduler

# OLLN
.PHONY: run-local-olln
run-local-olln:
	LUIGI_CONFIG_PATH=./olln.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb25121ac --local-scheduler

# Acropole
.PHONY: run-local-acropole
run-local-acropole:
	luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --local-scheduler

.PHONY: import-local
import-local:
	LUIGI_CONFIG_PATH=./dison.cfg $(LUIGI_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-dison --local-scheduler

test:
	nosetests imio_luigi

acropole-db:
	docker-compose -f docker-compose-acropole.yml -p acropole up
