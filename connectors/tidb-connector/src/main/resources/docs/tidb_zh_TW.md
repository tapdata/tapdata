## **连接配置帮助**

### **1. TiDB 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 TiDB 数据库。

### **2. 支持版本**
TiDB 5.x

### **3. 先决条件（作为源）**
对于某个数据库赋于select权限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. 先决条件（作为目标）**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```
