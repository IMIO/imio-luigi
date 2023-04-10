init:
	virtualenv-3.8 .
	bin/pip install -r requirements.txt

update:
	bin/pip install -r requirements.txt

.PHONY: run-local-dison
run-local-dison:
	LUIGI_CONFIG_PATH=$(CURDIR)/dison.cfg bin/luigi --module imio_luigi.urban.export_dison dison.GetFromAccess --filepath ./data/dison/AgoraWin/json/URBA.test.json --local-scheduler

.PHONY: run-local-acropole
run-local-acropole:
	bin/luigi --module imio_luigi.urban.export_acropole acropole.GetFromMySQL --local-scheduler

.PHONY: import-local
import-local:
	LUIGI_CONFIG_PATH=./dison.cfg bin/luigi --module imio_luigi.urban.importer urban.GetFiles --path ./result-dison --local-scheduler

test:
	bin/nosetests imio_luigi
