## **Connection configuration help**
### **1.  OpenGauss installation instructions**
Please follow the instructions below to ensure that the  OpenGauss database is successfully added and used in Tapdata.
### **2. Supported version**
OpenGauss3.0.0+
### **3. CDC principle and support**
```
host: xxxx
port: xxxx
database : postgres(OpenGauss automatic generation)
schema : public
user : tapdata
password : tapdata
logPluginName : PGOUTPUT (Other plugins do not support maintaining the default option or incremental function)
```
### **4. Prerequisites**
#### **4.1 Modify REPLICA IDENTITY**
This attribute determines the field of the log record when the data occurs `UPDATE, DELETE`
- **DEFAULT**-Updates and deletes will contain the current value of the primary key column
- **NOTHING**-Updates and deletes will not contain any previous values
- **FULL**-Updates and deletes will include the previous values of all columns
- **INDEX index name**-update and delete events will contain the previous value of the column included in the index definition named index name
  If there are scenarios where multiple tables are merged and synchronized, Tapdata needs to adjust this attribute to FULL
  Example
```
alter table'[schema]'.'[table name]' REPLICA IDENTITY FULL`
```


#### **4.2 Permissions**
##### **4.2.1 as source**
- **Initialization**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```

##### **4.2.2 as a target**
```
GRANT INSERT, UPDATE, DELETE, TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **Note**: The above are just basic permissions settings, the actual scene may be more complicated


#### **4.3 Incremental synchronization using the last update timestamp**
##### **4.3.1 Noun Explanation**
**schema**: Chinese is the model, pgsql has a total of 3 levels of directories, library -> model -> table. In the following command, the <schema> character needs to be filled in the name of the model where the table is located
### **5. Full type field support**
- smallint
- integer
- bigint
- numeric
- real
- double precision
- character
- character varying
- text
- bytea
- bit
- bit varying
- boolean
- date
- interval
- timestamp
- timestamp with time zone
- point
- line
- lseg
- box
- path
- polygon
- circle
- cidr
- inet
- macaddr
- uuid
- xml
- json
- tsvector (cdc not supported, no error)
- tsquery (cdc not supported, no error)
- oid
- regproc (cdc not supported, no error)
- regprocedure (cdc not supported, no error)
- regoper (cdc not supported, no error)
- regoperator (cdc not supported, no error)
- regclass (cdc not supported, no error)
- regtype (cdc not supported, no error)
- regconfig (cdc not supported, no error)
- regdictionary (cdc not supported, no error)
