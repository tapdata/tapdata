## **連接配置幫助**
###  **1. MongoDB Atlas安裝說明**
連接MongoDB Atlas時，需要按照MongoDB Atlas數據庫連接的URI示範格式填寫連接串。
> **注意**：連接MongoDB Atlas時，需要按照MongoDB Atlas數據庫連接的URI示範格式填寫連接串，連接串需要指定：用戶名、密碼、數據庫名。
#### **2. 支持版本**
MongoDB Atlas 5.0.15
> **注意**：請儘量保證資源端數據庫和目標數據庫都是5.0以上版本。
###  **3. 先決條件**
#### **3.1 作爲源數據庫**
##### **3.1.1 帳戶權限**
如果源端 MongoDB Atlas啓用了安全身份驗證，則 Tapdata 用於連接源端 MongoDB Atlas的用戶帳戶必須具有以下內置角色：
```
readAnyDatabase@admin
```
要創建具有上述權限的用戶，您可以參考以下：
```
在Atlas管理界面菜單欄中，選擇Database Access,之後點擊 "ADD NEW DATABASE USER" 按鈕，添加用戶並賦予相關權限。
```
##### **3.1.2 獲取URL**
創建用戶後，您可以：
```
菜單欄選擇Database，之後依次點擊 "Connect" --> "Connect your application"
便可以獲取URL。
```
URL格式：
```
mongodb+srv://<username>:<password>@atlascluster.bo1rp4b.mongodb.net/<databaseName>?retryWrites=true&w=majority
```
#### **3.2 作爲目標數據庫**
#####  **3.2.1 基本配置**
如果目標端 MongoDB Atlas啓用了安全身份驗證，則 Tapdata 用於連接源端 MongoDB Atlas的用戶帳戶必須具有以下內置角色：
```
readWriteAnyDatabase@admin
```
#### **4. MongoDB Atlas TLS/SSL配置**
- **啓用TLS/SSL**<br>
  請在左側配置頁的 “使用TLS/SSL連接”中選擇“是”項進行配置<br>
- **設置MongoDB PemKeyFile**<br>
  點擊“選擇文件”，選擇證書文件，若證書文件有密碼保護，則在“私鑰密碼”中填入密碼<br>
- **設置CAFile**<br>
  請在左側配置頁的 “驗證服務器證書”中選擇“是”<br>
  然後在下方的“認證授權”中點擊“選擇文件”<br>
- **TSL/SSL參考文檔**<br>
  https://www.mongodb.com/docs/atlas/setup-cluster-security/?_ga=2.260151054.2057403045.1679910300-992025068.1669632542#unified-aws-access
