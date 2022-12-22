# **連接配置幫助**

### **1. MYSQL 安裝說明**

請遵循以下說明以確保在 Tapdata 中成功添加和使用MySQL數據庫。

### **2. 支持版本**
MySQL 5.0、5.1、5.5、5.6、5.7、8.x

### **3. 先決條件（作為源）**
#### **3.1 開啟 Binlog**
- 必須開啟 MySQL 的 binlog ，Tapdata 才能正常完成同步工作。
- 級連刪除（CASCADE DELETE），這類由數據庫產生的刪除不會記錄在binlog內，所以不被支持。
  修改 `$MYSQL_HOME/mysql.cnf `, 例如:
```
server_id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 1
binlog_format     = row
binlog_row_image  = full
```
配置解釋：<br>
server-id: 對於 MySQL 中的每個服務器和復制客戶端必須是唯一的<br>
binlog_format：必須設置為 row 或者 ROW<br>
binlog_row_image：必須設置為 full<br>
expire_logs_days：二進制日誌文件保留的天數，到期會自動刪除<br>
log_bin：binlog 序列文件的基本名稱<br>

#### **3.2 重啟 MySQL**

```
/etc/inint.d/mysqld restart
```
驗證 binlog 已啟用，請在 mysql shell 執行以下命令
```
show variables like 'binlog_format';
```
輸出的結果中，format value 應該是"ROW"

驗證 binlog_row_image 參數的值是否為full:
```
show variables like 'binlog_row_image';
```
輸出結果中，binlog_row_image value應該是"FULL"

#### **3.3 創建MySQL賬號**
Mysql8以後，對密碼加密的方式不同，請注意使用對應版本的方式，設置密碼，否則會導致無法進行增量同步
##### **3.3.1 5.x版本**
```
create user 'username'@'localhost' identified by 'password';
```
##### **3.3.2 8.x版本**
```
// 創建用戶
create user 'username'@'localhost' identified with mysql_native_password by 'password';
// 修改密碼
alter user 'username'@'localhost' identified with mysql_native_password by 'password';

```

#### **3.4 給 tapdata 賬號授權**
對於某個數據庫賦於select權限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
對於全局的權限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
#### **3.5 約束說明**
```
當從MySQL同步到其他異構數據庫時，如果源MySQL存在表級聯設置，因該級聯觸發產生的數據更新和刪除不會傳遞到目標。如需要在目標端構建級聯處理能力，可以視目標情況，通過觸發器等手段來實現該類型的數據同步。
```
###  **4. 先決條件（作為目標）**
對於某個數據庫賦於全部權限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
對於全局的權限
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
###  **5. 常見錯誤**

Unknown error 1044
如果權限已經grant了，但是通過tapdata還是無法通過測試連接，可以通過下面的步驟檢查並修復
```
SELECT host,user,Grant_priv,Super_priv FROM mysql.user where user='username';
//查看Grant_priv字段的值是否為Y
//如果不是，則執行以下命令
UPDATE mysql.user SET Grant_priv='Y' WHERE user='username';
FLUSH PRIVILEGES;
```