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

.PHONY: download-euler
download-euler:
	rsync -av --delete --compress --progress "euler.affinitic.be:/home/jchandelle/imio-luigi/results/result-$(ORG)/*" ./results/result-$(ORG)/
	rsync -av --delete --compress --progress "euler.affinitic.be:/home/jchandelle/imio-luigi/failures/$(ORG)-*" ./failures

.PHONY: upload-euler
upload-euler:
	rsync -av --delete --compress --progress ./results/result-$(ORG)/* "jchandelle@euler.affinitic.be:/home/jchandelle/imio-luigi/results/result-$(ORG)/"


# ---Acropole---
.PHONY: run-local-acropole
run-local-acropole:
	luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --local-scheduler

.PHONY: import-local
import-local:
	LUIGI_CONFIG_PATH=./dison.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-dison --local-scheduler


# ---Arlon---
.PHONY: run-local-arlon
run-local-arlon:
	LUIGI_CONFIG_PATH=$(CURDIR)/arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_arlon arlon.GetFromAccess --filepath ./data/arlon/json/REGISTRES.json --local-scheduler

.PHONY: import-arlon
import-arlon:
	LUIGI_CONFIG_PATH=./arlon.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-arlon --local-scheduler  --logging-conf-file logging_arlon.ini


# ---Bastonge---
.PHONY: run-local-bastogne
run-local-bastogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/bastogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb82003ac --local-scheduler


# ---Berloz---
.PHONY: run-local-berloz
run-local-berloz:
	LUIGI_CONFIG_PATH=$(CURDIR)/berloz.cfg $(BIN_PATH)luigi --count=10 --module imio_luigi.urban.export_berloz berloz.GetFromCSV --filepath ./data/berloz/donnees_traitees.csv --local-scheduler


# ---Braine Le Comte---
.PHONY: run-local-brainelecompte
run-local-brainelecompte:
	LUIGI_CONFIG_PATH=$(CURDIR)/brainelecompte.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_brainelecompte brainelecompte.GetFromCSV --filepath ./data/brainelecompte/permis.csv --local-scheduler


# ---Dison---
.PHONY: run-local-dison
run-local-dison:
	LUIGI_CONFIG_PATH=$(CURDIR)/dison.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_dison dison.GetFromAccess --counter="500" --filepath ./data/dison/AgoraWin/json/URBA.json --local-scheduler


# ---Ecaussinnes---
.PHONY: run-local-ecaussinnes
run-local-ecaussinnes:
	LUIGI_CONFIG_PATH=$(CURDIR)/ecaussinnes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_ecaussinnes ecaussinnes.GetFromXML --filepath ./data/ecaussinnes/ --local-scheduler

.PHONY: import-ecaussinnes
import-ecaussinnes:
	LUIGI_CONFIG_PATH=./ecaussinnes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-ecaussinnes --local-scheduler


# ---Faimes---
.PHONY: run-local-faimes
run-local-faimes:
	LUIGI_CONFIG_PATH=$(CURDIR)/faimes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_faimes faimes.GetFromAccess --filepath ./data/faimes/AgoraWin/json/URBA_missingg_permis.json --local-scheduler

.PHONY: import-faimes
import-faimes:
	LUIGI_CONFIG_PATH=./faimes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-faimes --local-scheduler


# ---Farciennes---
.PHONY: run-local-farciennes
run-local-farciennes:
	LUIGI_CONFIG_PATH=$(CURDIR)/farciennes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_farciennes farciennes.GetJSON --filepath ./data/farciennes/permis.json --local-scheduler

.PHONY: import-farciennes
import-farciennes:
	LUIGI_CONFIG_PATH=./farciennes.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-farciennes --local-scheduler


# ---Flemalle---
.PHONY: run-local-flemalle-notaries
run-local-flemalle-notaries:
	LUIGI_CONFIG_PATH=$(CURDIR)/flemalle.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_flemalle_notaire flemalle.GetFromCSV --filepath ./data/flemalle/File_V_rename.csv --local-scheduler

.PHONY: import-flemalle-notaries
import-flemalle-notaries:
	LUIGI_CONFIG_PATH=./flemalle.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-flemalle-notaries --local-scheduler --logging-conf-file logging_flemalle.ini


# ---Grace-Hollogne---
.PHONY: run-local-gracehollogne
run-local-gracehollogne:
	LUIGI_CONFIG_PATH=$(CURDIR)/gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_gracehollogne gracehollogne.GetFromAccess --filepath ./data/gracehollogne/customer_ouput_formated.json --local-scheduler

.PHONY: run-local-gracehollogne-env
run-local-gracehollogne-env:
	LUIGI_CONFIG_PATH=$(CURDIR)/gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_gracehollogne_env gracehollogne_env.GetJSON --filepath ./data/gracehollogne/environements/json/test_error_event.json --local-scheduler

.PHONY: import-local-gracehollogne
import-gracehollogne:
	LUIGI_CONFIG_PATH=./gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-gracehollogne --local-scheduler

.PHONY: import-local-gracehollogne-env
import-gracehollogne-env:
	LUIGI_CONFIG_PATH=./gracehollogne.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --limit-hour --path ./results/result-gracehollogne-env --local-scheduler --logging-conf-file logging_gracehollogne.ini


# ---Hensies---
.PHONY: run-local-hensies
run-local-hensies:
	LUIGI_CONFIG_PATH=./hensies.cfg $(BIN_PATH)luigi --orga=hensies --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb53039ac --local-scheduler

.PHONY: import-hensies
import-hensies:
	LUIGI_CONFIG_PATH=./hensies.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --limit-hour --path ./results/result-hensies --local-scheduler  --logging-conf-file logging_hensies.ini


# ---Lierneux---
.PHONY: run-local-lierneux
run-local-lierneux:
	LUIGI_CONFIG_PATH=./lierneux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_lierneux lierneux.GetFromAccess --filepath ./data/lierneux/json/Urbi7/DOSSIERS.json  --local-scheduler

.PHONY: run-local-lierneux-architect
run-local-lierneux-architect:
	LUIGI_CONFIG_PATH=./lierneux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_lierneux_architect lierneux-architect.GetFromAccess --filepath ./data/lierneux/json/Urbi7/DOSSIERS.json  --local-scheduler

.PHONY: import-lierneux
import-lierneux:
	LUIGI_CONFIG_PATH=./lierneux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-lierneux --local-scheduler --logging-conf-file logging_lierneux.ini

.PHONY: import-lierneux-architecte
import-lierneux-architecte:
	LUIGI_CONFIG_PATH=./lierneux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-lierneux-architect --local-scheduler


# ---Mouscron---
.PHONY: run-local-mouscron
run-local-mouscron:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_mouscron mouscron.GetFromCSV --filepath ./data/mouscron/p/p_permis_rename.csv --local-scheduler --path-ref-to-import ./data/mouscron/missing_ref_to_export.json

.PHONY: run-local-mouscron-architecte
run-local-mouscron-architecte:
	LUIGI_CONFIG_PATH=$(CURDIR)/mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_mouscron_architect mouscron-architect.GetFromCSV --filepath ./data/mouscron/c/c_organisme.csv --local-scheduler

.PHONY: import-mouscron-architecte
import-mouscron-architecte:
	LUIGI_CONFIG_PATH=./mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-mouscron-architect --local-scheduler

.PHONY: import-mouscron
import-mouscron:
	LUIGI_CONFIG_PATH=./mouscron.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-mouscron-temp --local-scheduler --logging-conf-file logging_mourscon.ini

.PHONY: download-mouscron
download-mouscron:
	ORG=mouscron make download-euler


# ---OLLN---
.PHONY: run-local-olln
run-local-olln:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --orga=olln --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --dbname=urb25121ac --local-scheduler

.PHONY: run-local-olln-lotissements
run-local-olln-lotissements:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_olln_lotissements olln.GetFromCSV --filepath ./data/olln/lotissements/lotissements-olln.csv --local-scheduler

.PHONY: import-olln
import-olln:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-olln-missing-mai-2024 --local-scheduler

.PHONY: import-olln-lotissements
import-olln-lotissements:
	LUIGI_CONFIG_PATH=./olln.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/results/result-olln-lotissements --getter=label --object-provides=imio.urban.core.contents.parcelling.content.IParcelling --metadata-fields=label --local-scheduler


# ---Tellin---
.PHONY: run-local-tellin
run-local-tellin:
	LUIGI_CONFIG_PATH=$(CURDIR)/tellin.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_tellin tellin.GetFromAccess --filepath ./data/tellin/json/DOSSIERS.json --local-scheduler


# ---Theux---
.PHONY: run-local-theux
run-local-theux:
	LUIGI_CONFIG_PATH=./theux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_theux theux.GetFromAccess --filepath ./data/theux/20200225/json/DOSSIERS.json  --local-scheduler

.PHONY: import-theux
import-theux:
	LUIGI_CONFIG_PATH=./theux.cfg $(BIN_PATH)luigi --module imio_luigi.urban.importer urban.GetFiles --path ./results/result-theux --local-scheduler


# ---Vresse-sur-semois---
.PHONY: run-local-vressesursemois
run-local-vressesursemois:
	LUIGI_CONFIG_PATH=./vressesursemois.cfg $(BIN_PATH)luigi --module imio_luigi.urban.export_vressesursemois vressesursemois.GetFromExcelFile --filepath ./data/vressesursemois/permis.xls  --local-scheduler


test:
	nosetests imio_luigi

acropole-db:
	docker-compose -f docker-compose-acropole.yml -p acropole up

.PHONY: run-local-libreoffice-upgrade-templates
run-local-libreoffice-upgrade-templates:
	LUIGI_CONFIG_PATH=./config/local.cfg luigi --module imio_luigi.urban.libreoffice libreoffice.UpgradeTemplatesTask --local-scheduler
