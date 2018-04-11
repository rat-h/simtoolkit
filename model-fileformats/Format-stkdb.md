##STKDB
Simulation Tool Kit Data Base (STKDB) holds parameters and some results of simulations in a standard sqlight3 file or in an external database.
The main purpose of this database is an accumulation of parameter sets, some lightweight simulation results (such as some final numbers, mean firing rate for example), and possibly some multimedia content along with message related to each simulation for many simulations. One can imagine stkdb as git repository for simulations with additional features.

stkdb contains 5 tables: records, names, values, tags, mms

### Tables:
**table _stkrecords_** has columns:
1. id        INTEGER PRIMARY KEY AUTOINCREMENT - record ID (simulation ID)
2. timestamp DATETIME                          - record (simulation) date and time
3. hash      TEXT                              - record (simulation) hash sum
4. message   TEXT                              - message or comment on simulation result

**table _stknames_** has columns:
1. id        INTEGER PRIMARY KEY AUTOINCREMENT - parameter name ID 
2. name      TEXT                              - parameter name

**table _stkvalues_** has columns:
1. id       INTEGER PRIMARY KEY AUTOINCREMENT - value ID 
2. record   INTEGER                           - record ID where this parameter was set
3. name     INTEGER                           - ID of parameter name
4. type     TEXT   DEFAULT 'TEXT’             - how data is present. Can be 'TEXT' or 'ZIP' for zipped text or 'NUMPY' or 'ZIPNUMPY'.
5. value    BLOB                              - a value (text or zip or ?)

**table _stktags_** has columns:
1. id       INTEGER PRIMARY KEY AUTOINCREMENT - tag ID 
2. record   INTEGER                           - tagged record ID
3. tag      TEXT                              - a tag

**table _stkmms_** has columns:
1. id       INTEGER PRIMARY KEY AUTOINCREMENT - multimedia ID 
2. record   INTEGER                           - attached to record ID
3. format   TEXT                              - multimedia format 
4. name     TEXT                              - contant name
5. data     BLOB                              - contant data

