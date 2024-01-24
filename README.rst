iMio Luigi
==========

Acropole
--------

mysql --password=password -e "create database urb82003ac;"
mysql -p urb82003ac --password=password < /data/bastogne/urb82003ac.dmp
mysql -p urb82003ac --password=password < /data/acropole_view.sql

mysql --password=password -e "create database urb25121ac;"
mysql -p urb25121ac --password=password < /data/olln/urb25121ac.dmp
mysql -p urb25121ac --password=password < /data/olln/urb25121ac_index.dmp
mysql -p urb25121ac --password=password < /data/acropole_table.sql