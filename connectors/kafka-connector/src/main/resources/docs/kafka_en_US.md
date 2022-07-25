## **Connection configuration help**
### **1. Elastic Search Installation Instructions**
Please follow the instructions below to ensure that the Elastic Search database is successfully added and used in Tapdata.
### **2. Restrictions **
The current version of the Tapdata system Elastic Search only supports as a target, and the supported data source types are: Oracle, MySQL, MongoDB, PostgreSQL, and SQL Server.

|Source|Target|Support|
|:-----------:|:-----------:|:-----------:|
Oracle| Elastic Search | Support<br>
MySQL| Elastic Search |Support<br>
MongoDB| Elastic Search | Support<br>
PostgreSQL| Elastic Search | Support<br>
SQL Server | Elastic Search | Support<br>

### **3. Supported version**
Elastic search 7.6
### **4. Configuration Instructions**
- Host/IP
- Port
- Database name
- Cluster name
> **Special Instructions**<br>
> The Elastic Search password is not required, but if the Elastic Search database you want to configure has a password, and you have not configured the password in Tapdata, the test will fail.
>

### **5. Connection test items**
- Check host/IP and port
- Check account and password