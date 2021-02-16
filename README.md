# Kaggle Titanic - Oracle Machine Learning

This is an execution of Kaggle's legendary Titanic ML competition using Oracle Machine Learning (OML). As an Oracle DBA and aspiring Data Scientist, I was intrigued to learn about Oracle's built-in machine learning functionality. While OML does not offer that same level of flexibility as languages like Python or R, it does provide a fair number of ML tools for building models. By testing all the available classification models, I was able to identify the most accurate one. In fact, it scored slightly better than my last entry using Python.

### Prerequisites

Oracle 11.2g Enterprise Edition or higher (the Advanced Analytics option required a license prior to version 19c)
SQL Developer (version 4 or later)
DBA access to make system-level changes (create users, install additional options)

### Installing

(DBA) Enable the Advanced Analytics option

```
chopt enable dm
```

(DBA) Create a schema then create a connection in SQL Developer

```
CREATE USER dmuser IDENTIFIED BY password
DEFAULT TABLESPACE datamining
TEMPORARY TABLESPACE temp
QUOTA UNLIMITED ON datamining;

GRANT CONNECT, RESOURCE TO dmuser;
```

(DBA) Create the Oracle Data Mining Repository using SQL Developer

```
Navigate to View --> Data Miner --> Data Miner Connections
In the pop-up window choose the connection created above
When it completes, the dmuser will have all necessary privileges to create machine learning models
```

## Deployment

Connect to the database as dmuser (I used SQL Plus) and run the script.

```
@OML_titanic.sql
```

## Built With

* [Oracle VM VirtualBox](https://www.oracle.com/virtualization/virtualbox/) - The virtual software used
* [Oracle Developer Day - Hands-on Database Application Development](https://www.oracle.com/database/technologies/databaseappdev-vm.html) - The pre-built virtual machine used
* Oracle Database 19.3 - Database with built-in machine learning tools
* Oracle SQL Developer 19.1 - GUI tool required for installing required components

## Authors

* **Kevin DeJesus** - *Initial work* - [deg75kd](https://github.com/deg75kd)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* "Predictive Analytics Using Oracle Data Miner" by Brendan Tierney
* Oracle DBMS_DATA_MINING documentation (https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_DATA_MINING.html#GUID-7B9145D4-831F-46B3-977F-01AF77ACA4A1)
* Oracle DBMS_DATA_MINING_TRANSFORM documentation (https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_DATA_MINING_TRANSFORM.html#GUID-BF2452B5-2ADF-4EEB-81C8-8DDD3162586B)