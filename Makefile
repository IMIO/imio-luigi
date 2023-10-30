SHELL:=/bin/bash
PROJECT=imio-luigi
VERSION=3.10.12
VENV=${PROJECT}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python

init:
	pyenv virtualenv ${VERSION} ${VENV}
	echo ${VENV} > .python-version
	$(PYTHON) -m pip install install -r requirements.txt

update:
	$(PYTHON) -m pip install -r requirements.txt

.PHONY: run-local-dison
run-local-dison:
	LUIGI_CONFIG_PATH=$(CURDIR)/dison.cfg luigi --module imio_luigi.urban.export_dison dison.GetFromAccess --counter="500" --filepath ./data/dison/AgoraWin/json/URBA.json --local-scheduler

.PHONY: run-local-arlon
run-local-arlon:
	LUIGI_CONFIG_PATH=$(CURDIR)/arlon.cfg luigi --module imio_luigi.urban.export_arlon arlon.GetFromAccess --filepath ./data/arlon/json/REGISTRES.json --local-scheduler

.PHONY: clear-arlon
clear-arlon:
	rm -rf result-arlon/ failures/arlon-*


.PHONY: run-local-ecaussinnes
run-local-ecaussinnes:
	LUIGI_CONFIG_PATH=$(CURDIR)/ecaussinnes.cfg luigi --module imio_luigi.urban.export_ecaussinnes ecaussinnes.GetFromXML --filepath ./data/ecaussinnes/ --local-scheduler

.PHONY: clear-ecaussinnes
clear-ecaussinnes:
	rm -rf result-ecaussinnes/ failures/ecaussinnes-*

.PHONY: run-local-acropole
run-local-acropole:
	luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --local-scheduler

.PHONY: import-local
import-local:
	LUIGI_CONFIG_PATH=./dison.cfg luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-dison --local-scheduler

test:
	nosetests imio_luigi

acropole-db:
	docker-compose -f docker-compose-acropole.yml -p acropole up
