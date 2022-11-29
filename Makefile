init:
	virtualenv-3.8 .
	bin/pip install -r requirements.txt

test:
	bin/nosetests imio_luigi
