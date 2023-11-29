## **connection configuration description**

### **1. Oceanbase Installation instructions.**

Please follow the instructions below to ensure successful addition and usage of the Oceanbase database in Tapdata.

### **2. Supported versions**
Oceanbase 5.0、5.1、5.5、5.6、5.7、8.x

### **3. Prerequisites (as the source)**
#### **3.1 Enable Binlog.**
- Binlog must be enabled on Oceanbase for Tapdata to successfully complete synchronization tasks.
- Cascade deletion (CASCADE DELETE) generated by the database is not supported because it is not recorded in the binlog.
  Modify `$Oceanbase_HOME/Oceanbase.cnf `, For example:
```
server_id         = 223344
log_bin           = Oceanbase-bin
expire_logs_days  = 1
binlog_format     = row
binlog_row_image  = full
```
Configuration explanation：<br>
server-id: For Oceanbase, each server and replication client must have a unique identifier.<br>
binlog_format：It must be set to "row" or "ROW".<br>
binlog_row_image：It must be set to "full".<br>
expire_logs_days：The number of days to retain binary log files. They will be automatically deleted upon expiration.<br>
log_bin：The base name of the binlog sequence files.<br>

#### **3.2 Restart Oceanbase**

```
/etc/inint.d/Oceanbased restart
```
To verify that binlog is enabled, please execute the following command in the Oceanbase shell:
```
show variables like 'binlog_format';
```
In the output result, the format value should be 'ROW'.

Verify if the value of the binlog_row_image parameter is 'full'.
```
show variables like 'binlog_row_image';
```
In the output result, the binlog_row_image value should be 'FULL'.

#### **3.3 Create an Oceanbase account.**
Starting from OceanBase 8, the password encryption method has changed. Please ensure that you use the appropriate method for the corresponding version when setting the password. Otherwise, it may result in the inability to perform incremental synchronization.
To confirm whether supplemental logging is enabled, use the following command:
##### **Version 3.3.1 5.x**
```
create user 'username'@'localhost' identified by 'password';
```
##### **Version 3.3.2 8.x**
```
// create user
create user 'username'@'localhost' identified with Oceanbase_native_password by 'password';
// Modify the password.
alter user 'username'@'localhost' identified with Oceanbase_native_password by 'password';

```

#### **3.4 Grant permissions to the Tapdata account.**
Grant the SELECT privilege to a specific database.
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
Grant global privileges.
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
#### **3.5 Constraint explanation**
```
When synchronizing from OceanBase to other heterogeneous databases, if the source OceanBase has table-level cascade settings, the data updates and deletions generated by such cascading triggers will not be propagated to the destination. If you need to build cascade processing capability at the destination, you can consider implementing this type of data synchronization through triggers or other means, depending on the specific requirements of the target system.
```
###  **4. Prerequisites (as the target)**
Grant all privileges to a specific database.
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
Grant global privileges.
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
###  **5. Common errors.**

Unknown error 1044
If permissions have already been granted but you're still unable to establish a successful connection through Tapdata, you can follow the steps below to check and resolve the issue:
```
SELECT host,user,Grant_priv,Super_priv FROM Oceanbase.user where user='username';
//To check if the Grant_priv field's value is 'Y'
//If the Grant_priv field is not set to 'Y', you can execute the following command to grant the necessary privileges:
UPDATE Oceanbase.user SET Grant_priv='Y' WHERE user='username';
FLUSH PRIVILEGES;
```