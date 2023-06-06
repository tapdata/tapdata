## **連接配寘幫助**

### **1. Aliyun ADB MySQL 安裝說明**

請遵循以下說明以確保在Tapdata中成功添加和使用Aliyun ADB MySQL資料庫。

### **2. 支持版本**
Aliyun ADB MySQL 5.0、5.1、5.5、5.6、5.7、8.x

### **3. 先決條件（作為源）**
#### **3.1 開啟 Binlog**
- 必須開啟Aliyun ADB MySQL的binlog，Tapdata才能正常完成同步工作。
- 級連删除（CASCADE DELETE），這類由資料庫產生的删除不會記錄在binlog內，所以不被支持。
  修改`$MYSQL_ HOME/mysql.cnf `，例如:
```
server_id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 1
binlog_format     = row
binlog_row_image  = full
```
配寘解釋：<br>
server-id:  對於Aliyun ADB MySQL中的每個服務器和複製用戶端必須是唯一的 <br>
binlog_format： 必須設定為row或者ROW <br>
binlog_row_image：必須設定為 full<br>
expire_logs_days： 二進位日誌檔保留的天數，到期會自動删除 <br>
log_bin：binlog  序列檔案的基本名稱 <br>

#### **3.2 重啓 Aliyun ADB MySQL**

```
/etc/inint.d/mysqld restart
```
驗證binlog已啟用，請在mysql shell執行以下命令
```
show variables like 'binlog_format';
```
輸出的結果中，format value應該是 "ROW"

驗證binlog_row_image參數的值是否為 full:
```
show variables like 'binlog_row_image';
```
輸出結果中，binlog_row_image value應該是 "FULL"

#### **3.3  創建Aliyun ADB MySQL帳號**

Mysql8以後，對密碼加密的管道不同，請注意使用對應版本的管道，設置密碼，否則會導致無法進行增量同步 

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

#### **3.4  給tapdata帳號授權**

對於某個資料庫賦於select許可權

```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```

對於全域的許可權

```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```

#### **3.5 約束說明**

```
 當從Aliyun ADB MySQL同步到其他異構資料庫時，如果源Aliyun ADB MySQL存在錶級聯設定，因該級聯觸發產生的數據更新和删除不會傳遞到目標。 如需要在目標端構建級聯處理能力，可以視目標情况，通過觸發器等手段來實現該類型的資料同步。 ```
```

###  **4. 關於更新事件**
AliYun ADB Mysql更新事件不可更新主鍵，囙此寫入需要判斷修改前和修改後的主鍵值是否相同，相同時需要移除主鍵進行修改，不相同則拆成删除和插入處理 