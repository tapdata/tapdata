## **連接配寘幫助**
### **1. Aliyun RDS MySQL安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用Aliyun RDS MySQL資料庫。
### **2. 支持版本**
Aliyun RDS MySQL 5.0、5.1、5.5、5.6、5.7、8.x
### **3. 先決條件（作為源）**
#### **3.1開啟Binlog**
-必須開啟Aliyun RDS MySQL的binlog，Tapdata才能正常完成同步工作。
-級連删除（CASCADE DELETE），這類由資料庫產生的删除不會記錄在binlog內，所以不被支持。
修改`$MYSQL_ HOME/mysql.cnf `，例如：
```
server_ id = 223344
log_ bin = mysql-bin
expire_ logs_ days = 1
binlog_ format = row
binlog_ row_ image = full
```
配寘解釋：<br>
server-id:對於Aliyun RDS MySQL中的每個服務器和複製用戶端必須是唯一的<br>
binlog_ format：必須設定為row或者ROW<br>
binlog_ row_ image：必須設定為full<br>
expire_ logs_ days：二進位日誌檔保留的天數，到期會自動删除<br>
log_ bin:binlog序列檔案的基本名稱<br>
#### **3.2重啓Aliyun RDS MySQL**
```
/etc/inint.d/mysqld restart
```
驗證binlog已啟用，請在mysql shell執行以下命令
```
show variables like 'binlog_ format'；
```
輸出的結果中，format value應該是“ROW”
驗證binlog_ row_ image參數的值是否為full:
```
show variables like 'binlog_ row_ image'；
```
輸出結果中，binlog_ row_ image value應該是“FULL”
#### **3.3創建Aliyun RDS MySQL帳號**
Mysql8以後，對密碼加密的管道不同，請注意使用對應版本的管道，設置密碼，否則會導致無法進行增量同步
##### **3.3.1 5.x版本**
```
create user 'username'@'localhost' identified by 'password'；
```
##### **3.3.2 8.x版本**
```
//創建用戶
create user 'username'@'localhost' identified with mysql_ native_ password by 'password'；
//修改密碼
alter user 'username'@'localhost' identified with mysql_ native_ password by 'password'；
```
#### **3.4給tapdata帳號授權**
對於某個資料庫賦於select許可權
```
GRANT SELECT，SHOW VIEW，CREATE ROUTINE，LOCK TABLES ON <DATABASE_ NAME>.< TABLE_ NAME> TO 'tapdata' IDENTIFIED BY 'password'；
```
對於全域的許可權
```
GRANT RELOAD，SHOW DATABASES，REPLICATION SLAVE，REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password'；
```
#### **3.5約束說明**
```
當從Aliyun RDS MySQL同步到其他異構資料庫時，如果源Aliyun RDS MySQL存在錶級聯設定，因該級聯觸發產生的數據更新和删除不會傳遞到目標。 如需要在目標端構建級聯處理能力，可以視目標情况，通過觸發器等手段來實現該類型的資料同步。
```
### **4. 先決條件（作為目標）**
對於某個資料庫賦於全部許可權
```
GRANT ALL PRIVILEGES ON <DATABASE_ NAME>.< TABLE_ NAME> TO 'tapdata' IDENTIFIED BY 'password'；
```
對於全域的許可權
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password'；
```
### **5. 常見錯誤**
Unknown error 1044
如果許可權已經grant了，但是通過tapdata還是無法通過測試連接，可以通過下麵的步驟檢查並修復
```
SELECT host，user，Grant_ priv，Super_ priv FROM mysql.user where user='username'；
//查看Grant_ priv欄位的值是否為Y
//如果不是，則執行以下命令
UPDATE mysql.user SET Grant_ priv='Y' WHERE user='username'；
FLUSH PRIVILEGES；
```