## **连接配置帮助**
### **1. TiDB 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 TiDB 数据库以及成功部署TiCDC。

### **2. 支持版本**
TiDB 5.4+ , TiCDC6.3+

### **3. 先决条件（作为源）**
3.1配置连接示例

3.1.1未开启增量配置
```
PdServer 地址：192.168.1.179:2379
数据库地址：192.168.1.179
端口：4000
数据库名称：kiki
账号：root
密码：root
```
3.1.2开启增量只需要追加以下配置
```
kafka地址：139.198.127.226:32761
kafka主题：tidb-cdc
ticdc地址：192.168.1.179:8300

```

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
