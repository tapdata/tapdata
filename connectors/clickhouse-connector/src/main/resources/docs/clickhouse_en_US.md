## **Connection configuration help**
### **1. Clickhouse installation instructions**
Please follow the instructions below to ensure that the PostgreSQL database is successfully added and used in IKAS.
### **2. Supported version**
ClickHouse v21.x

### **3. 不支持字段说明**
ClickHouse does not support binary-related field types. If you have fields of related types in your source table, you can delete them in the field mapping settings, otherwise the task may not work properly. 