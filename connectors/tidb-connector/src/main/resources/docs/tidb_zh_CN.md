## **连接配置帮助**
### **1. TiDB 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 TiDB 数据库以及成功部署TiCDC,建议TIDB与TICDC版本一致。

### **2. 支持版本**
TiDB 5.4+ , TiCDC6.3+

### **3. 先决条件（作为源）**
3.1配置连接示例

3.1.1未开启增量配置
```
PdServer 地址：xxxx:xxxx
数据库地址：xxxx
端口：xxxx
数据库名称：xxxx
账号：xxxx
密码：xxxx
```
3.1.2开启增量只需要追加以下配置
```
kafka地址：xxxx:xxxx
kafka主题：xxxx
ticdc地址：xxxxx:xxxx

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
