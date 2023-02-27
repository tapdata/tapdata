## **連接配置幫助**

### **1. TiDB 安裝説明**

請遵循以下説明以確保在 Tapdata 中成功添加和使用 TiDB數據庫以及成功部署TiCDC。
### **2. 支持版本**
TiDB 5.4+ , TiCDC6.3+

### **3. 先決條件（作为源）**

3.1配置連接示例

3.1.1未開啓增量配置
```
PdServer 地址：192.168.1.179:2379
數據庫地址：192.168.1.179
端口：4000
數據庫名稱：kiki
账号：root
密码：root
```
3.1.2開啓增量配置只需要追加以下配置
```
kafka地址：139.198.127.226:32761
kafka主題：tidb-cdc
ticdc地址：192.168.1.179:8300

```

对于某个數據庫赋于select權限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局權限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. 先决条件（作为目标）**
对于某个數據庫赋于全部權限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的權限
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```
