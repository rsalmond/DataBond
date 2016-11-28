# Data Hoser
Suck data from database into another.

## Overview

**What does this do?**

Datahoser copies data from one relational database to another.

**Why?**

Searching for information on moving data from one database to another yields a variety of recommended approaches. There are open source but [database specific](http://dev.mysql.com/doc/workbench/en/wb-admin-export-import-table.html) [import tools](http://pgloader.io/) which may or may not import your current data format of choice. There are (frankly not awesome looking) commercial products, and there are quite a few janky search/and/replace your database dump scripts to convert on dump format into another. When I had a need to convert a sqlite database from a small project into a more robust RDBS I wanted something reliable.

**How does it work?**

By building on [the shoulders of giants](http://www.sqlalchemy.org/). SQLAlchemy has, at the time of this writing, [support for six RDBS engines](http://docs.sqlalchemy.org/en/latest/dialects/) which means that Datahoser does too. Datahoser builds on the powerful [reflection](http://docs.sqlalchemy.org/en/latest/core/reflection.html) methods to interrogate an existing database and build representations of it which are then used to replicate it's structure and contents in the destination database.

## Usage

*note*: these examples make use of the freely available example [Chinook database](http://chinookdatabase.codeplex.com/).

```
$ time python datahoser.py --sourcedb sqlite:///Chinook.sqlite --destdb mysql+pymysql://root@localhost --encoding=utf8
INFO:__main__:Operating at log level: 20
INFO:__main__:Creating destination database.
INFO:__main__:Creating 11 tables in dest database.
INFO:__main__:Importing table `Playlist`.
INFO:__main__:Importing table `Artist`.
INFO:__main__:Importing table `MediaType`.
INFO:__main__:Importing table `Genre`.
INFO:__main__:Importing table `Employee`.
INFO:__main__:Importing table `Album`.
INFO:__main__:Importing table `Customer`.
INFO:__main__:Importing table `Track`.
/home/vagrant/datahoser/.venv/local/lib/python2.7/site-packages/SQLAlchemy-1.1.5.dev0-py2.7-linux-x86_64.egg/sqlalchemy/sql/sqltypes.py:596: SAWarning: Dialect sqlite+pysqlite does *not* support Decimal objects natively, and SQLAlchemy must convert from floating point - rounding errors and other issues may occur. Please consider storing Decimal numbers as strings or integers on this platform for lossless storage.
INFO:__main__:Importing table `Invoice`.
INFO:__main__:Importing table `InvoiceLine`.
INFO:__main__:Verifying the contents of table `Album`.
INFO:__main__:347 rows out of 347 verified identical in source and destination table Album
INFO:__main__:Verifying the contents of table `Customer`.
INFO:__main__:59 rows out of 59 verified identical in source and destination table Customer
INFO:__main__:Verifying the contents of table `Playlist`.
INFO:__main__:18 rows out of 18 verified identical in source and destination table Playlist
INFO:__main__:Verifying the contents of table `Artist`.
INFO:__main__:275 rows out of 275 verified identical in source and destination table Artist
INFO:__main__:Verifying the contents of table `Track`.
INFO:__main__:3503 rows out of 3503 verified identical in source and destination table Track
INFO:__main__:Verifying the contents of table `Employee`.
INFO:__main__:8 rows out of 8 verified identical in source and destination table Employee
INFO:__main__:Verifying the contents of table `MediaType`.
INFO:__main__:5 rows out of 5 verified identical in source and destination table MediaType
INFO:__main__:Verifying the contents of table `InvoiceLine`.
INFO:__main__:2240 rows out of 2240 verified identical in source and destination table InvoiceLine
INFO:__main__:Verifying the contents of table `Invoice`.
INFO:__main__:412 rows out of 412 verified identical in source and destination table Invoice
INFO:__main__:Verifying the contents of table `Genre`.
INFO:__main__:25 rows out of 25 verified identical in source and destination table Genre
INFO:__main__:Verification successful, every table, column, and row present in the source db is present in the destination db.

real  0m25.624s
user  0m17.496s
sys 0m3.104s
```
