##STKDB
Simulation Tool Kit Data Base (STKDB) holds parameters and some results of simulations in a standard sqlight3 data base.

stkdb contains 5 tables: records, names, values, tags, mms

### Tables:
**table _stkrecords_** has columns:
1. id        INTEGER PRIMARY KEY AUTOINCREMENT - record ID
2. timestamp DATETIME                          - record data and time
3. hash      TEXT                              - record hash sum
4. message   TEXT                              - message or comment on simulation or files update

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
3. data     BLOB                              - data

