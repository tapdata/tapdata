## **Connection Configuration Help**

### **1. TiDB installation instructions**


Please follow the instructions below to ensure that TiDB database is successfully added and used in Tapdata and TiCDC is successfully deployed，it is recommended that TIDB and TICDC versions be consistent.

### **2. Supported versions**
TiDB 5.4+ , TiCDC6.3+
### **3. Prerequisites (as source)**
3.1 Example of configuration connection
3.1.1 Incremental configuration is not enabled
```
PdServer address: xxxx:xxxx
Database address: xxxx
Port: xxxx
Database name: xxxx
Account: xxxx
Password: xxxx
```
3.1.2 Only the following configurations need to be added to enable the increment
```
Kafka address: xxxx:xxxx
Kafka Subject: xxxx
Ticdc address: xxxx:xxxx
```

Assign select permission to a database
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
Permissions for global
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. Prerequisites (as targets)**
Assign all permissions to a database
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
Permissions for global
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```
