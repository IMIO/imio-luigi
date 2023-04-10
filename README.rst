iMio Luigi
==========

Acropole
--------

mysql --password=password -e "create database urb82003ac;"
mysql -p urb82003ac --password=password < /data/Bastogne/20211029/misc/dump/backup_mysql51/urb82003ac.dmp
mysql -p urb82003ac --password=password < /data/acropole_view.sql
