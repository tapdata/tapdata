## **連接配寘幫助**
### **1. Doris安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用Doris資料庫。
### **2. 支持版本**
Doris 1.x
### **3. 先決條件**
#### **3.1創建Doris帳號**
```
//創建用戶
create user 'username'@'localhost' identified with Doris_ native_ password by 'password'；
//修改密碼
alter user 'username'@'localhost' identified with Doris_ native_ password by 'password'；
```
#### **3.2給tapdata帳號授權**
對於某個資料庫賦於select許可權
```
GRANT SELECT，SHOW VIEW，CREATE ROUTINE，LOCK TABLES ON <DATABASE_ NAME>.< TABLE_ NAME> TO 'tapdata' IDENTIFIED BY 'password'；
```
對於全域的許可權
```
GRANT RELOAD，SHOW DATABASES，REPLICATION SLAVE，REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password'；
```
#### **3.3約束說明**
```
當從Doris同步到其他異構資料庫時，如果源Doris存在錶級聯設定，因該級聯觸發產生的數據更新和删除不會傳遞到目標。 如需要在目標端構建級聯處理能力，可以視目標情况，通過觸發器等手段來實現該類型的資料同步。
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
SELECT host，user，Grant_ priv，Super_ priv FROM Doris.user where user='username'；
//查看Grant_ priv欄位的值是否為Y
//如果不是，則執行以下命令
UPDATE Doris.user SET Grant_ priv='Y' WHERE user='username'；
FLUSH PRIVILEGES；
```