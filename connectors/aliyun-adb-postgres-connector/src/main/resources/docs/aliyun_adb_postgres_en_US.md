## **Connection configuration help**
### **1. POSTGRESQL installation instructions**
Please follow the instructions below to ensure that the PostgreSQL database is successfully added and used in Tapdata.
### **2. Supported version**
PostgreSQL 9.4, 9.5, 9.6, 10.x, 11.x, 12 versions
### **3. CDC principle and support**
#### **3.1 Principles of CDC**
PostgreSQL's logical decoding function first appeared in version 9.4. It is a mechanism that allows to extract the changes committed to the transaction log and process these changes in a user-friendly way through the output plugin.
This output plugin must be installed before running the PostgreSQL server and enabled with a replication slot so that the client can use the changes.
#### **3.2 CDC Support**
- **Logical Decoding** (Logical Decoding): used to parse logic change events from WAL logs
- **Replication Protocol** (Replication Protocol): Provides a mechanism for consumers to subscribe to (or even synchronously subscribe) database changes in real time
- **Export snapshot**: Allows to export a consistent snapshot of the database (pg_export_snapshot)
- **Replication Slot**: Used to save consumer offsets and track subscriber progress.
  Therefore, based on the above, we need to install a logical decoder. The existing decoders are as options

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

#### **4.2 Plug-in installation**
- [decorderbufs](https://github.com/debezium/postgres-decoderbufs)
- [Protobuf-c 1.2+](https://github.com/protobuf-c/protobuf-c)
- [protobuf](https://blog.csdn.net/gumingyaotangwei/article/details/78936608)
- [PostGIS 2.1+ ](http://www.postgis.net/)
- [wal2json](https://github.com/eulerto/wal2json/blob/master/README.md)
- pgoutput(pg 10.0+)

**Installation steps**<br>
Take wal2json as an example, the installation steps are as follows<br>
Ensure that the environment variable PATH contains "/bin"<br>
```
export PATH=$PATH:<postgres installation path>/bin
```
**Install plugin**<br>
```
git clone https://github.com/eulerto/wal2json -b master --single-branch \
&& cd wal2json \
&& USE_PGXS=1 make \
&& USE_PGXS=1 make install \
&& cd .. \
&& rm -rf wal2json
```
Install plug-in error handling. The `make` command is executed, and an exception message similar to `fatal error: [xxx].h: No such file or directory` is encountered.<br>
**Reason**: missing postgresql-server-dev<br>
**Solution**: install postgresql-server-dev, take the debian system as an example<br>
```
// Version number such as: 9.4, 9.6, etc.
apt-get install -y postgresql-server-dev-<version number>
```
**Configuration File**<br>
If you are using a supported logic decoding plug-in (not pgoutput ), and it has been installed, configure the server to load the plug-in at startup:<br>
```
postgresql.conf
shared_preload_libraries ='decoderbufs,wal2json'
```
Configure replication<br>
```
# REPLICATION
wal_level = logical
max_wal_senders = 1 # More than 0 is enough
max_replication_slots = 1 # More than 0 is enough
```

#### **4.3 Permissions**
##### **4.3.1 as source**
- **Initialization**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
- **Increment**<br>
  The user needs to have the replication login permission. If the log increment function is not required, the replication permission may not be set
```
CREATE ROLE <rolename> REPLICATION LOGIN;
CREATE USER <username> ROLE <rolename> PASSWORD'<password>';
// or
CREATE USER <username> WITH REPLICATION LOGIN PASSWORD'<password>';
```
The configuration file pg_hba.conf needs to add the following content:<br>
```
pg_hba.conf
local replication <youruser> trust
host replication <youruser> 0.0.0.0/32 md5
host replication <youruser> ::1/128 trust
```

##### **4.3.2 as a target**
```
GRANT INSERT, UPDATE, DELETE, TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **Note**: The above are just basic permissions settings, the actual scene may be more complicated

##### **4.4 Test Log Plugin**
> **Note**: The following operations are recommended to be performed in a POC environment
>Connect to the postgres database, switch to the database that needs to be synchronized, and create a test table
```
- Assuming that the database to be synchronized is postgres and the model is public
\c postgres

create table public.test_decode
(
  uid integer not null
      constraint users_pk
          primary key,
  name varchar(50),
  age integer,
  score decimal
)
```
You can create a test table according to your own situation<br>
- Create a slot connection, take the wal2json plugin as an example
```
select * from pg_create_logical_replication_slot('slot_test','wal2json')
```
- After the creation is successful, insert a piece of data into the test table<br>
- Monitor the log, check the returned result, and see if there is any information about the insert operation just now<br>
```
select * from pg_logical_slot_peek_changes('slot_test', null, null)
```
-After success, destroy the slot connection and delete the test table<br>
```
select * from pg_drop_replication_slot('slot_test')
drop table public.test_decode
```
#### **4.5 Exception Handling**
- **Slot Cleanup**<br>
  If tapdata is interrupted due to an uncontrollable exception (power failure, process crash, etc.), the slot connection cannot be deleted from the pg master node correctly, and a slot connection quota will always be occupied. You need to manually log in to the master node to delete
  Query slot information
```
// Check if there is slot_name=tapdata information
 TABLE pg_replication_slots;
```
- **Delete slot node**<br>
```
select * from pg_drop_replication_slot('tapdata');
```
- **Delete operation**<br>
  When using the wal2json plug-in to decode, if the source table does not have a primary key, the delete operation of incremental synchronization cannot be achieved

#### **4.6 Incremental synchronization using the last update timestamp**
##### **4.6.1 Noun Explanation**
**schema**: Chinese is the model, pgsql has a total of 3 levels of directories, library -> model -> table. In the following command, the <schema> character needs to be filled in the name of the model where the table is located
##### **4.6.2 Pre-preparation (this step only needs to be done once)**
- **Create public function**
  In the database, execute the following command
```
CREATE OR REPLACE FUNCTION <schema>.update_lastmodified_column()
    RETURNS TRIGGER language plpgsql AS $$
    BEGIN
        NEW.last_update = now();
        RETURN NEW;
    END;
$$;
```
- **Create field and trigger**
> **Note**: The following operations need to be performed once for each table
Assume that the table whose last update needs to be added is named mytable
- **Create last_update field**
```
alter table <schema>.mytable add column last_udpate timestamp default now();
```
- **Create trigger**
```
create trigger trg_uptime before update on <schema>.mytable for each row execute procedure
    update_lastmodified_column();
```