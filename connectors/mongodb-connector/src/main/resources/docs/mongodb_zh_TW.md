## **連接配寘幫助**
### **1. MONGODB安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用MongoDB資料庫。
> **注意**:MongoDB作為源端連接時，必須是副本集。
#### **2. 支持版本**
MongoDB 3.2、3.4、3.6、4.0、4.2
>**注意**:<br>
>由於Tapdata資料同步現時是基於MongoDB的Change Stream支持對多錶合併的操作，而MongoDB官方是從4.0版本開始支持Change Stream的，囙此，請儘量保證源端資料庫和目標端資料庫都是4.0及以上版本。
### **3. 先決條件**
#### **3.1作為源資料庫**
##### **3.1.1基本配置**
-源端MongoDB支持副本集和分片集羣。
-如果源端MongoDB只有一個節點，您可以將其配寘為單成員的複製集，以開啟oplog功能。
-您應該配寘足够的oplog空間。 我們建議至少足以容納24小時的oplog。
##### **3.1.2帳戶許可權**
如果源端MongoDB啟用了安全身份驗證，則Tapdata用於連接源端MongoDB的使用者帳戶必須具有以下內寘角色：
```
clusterMonitor（讀取oplog）
readAnyDatabase
```
要創建具有上述許可權的用戶，您可以參考以下示例：
```
use admin
db.createUser（{
“user”：“johndoe”，
“pwd”：“my_password”，
“roles”：[
{
“role”：“clusterMonitor”，
“db”：“admin”
}，
{
“role”：“readAnyDatabase”，
“db”：“admin”
}
]
}
```
如果您不希望授予` readAnyDatabase `角色，則還可以向特定的資料庫以及local和config資料庫賦予讀取許可權。 例如：
```
use admin
db.createUser（{
“user”：“johndoe”，
“pwd”：“my_password”，
“roles”：[
{
“role”：“clusterMonitor”，
“db”：“admin”
}，
{
“role”：“read”，
“db”：“my_db”
}，
{
“role”：“read”，
“db”：“local”
}，
{
“role”：“read”，
“db”：“config”
}
]
}
```
請注意，只有MongoDB版本3.2需要local資料庫的讀取許可權。
> **重要事項**<br>
>對於集羣分片，您必須在每個分片主節點上創建適當的用戶許可權。 這是由於MongoDB的安全架構設計。
>當登入到每個單獨的分片時，分片服務器不會向config資料庫獲取用戶許可權。 相反，它將使用其本地用戶資料庫進行身份驗證和授權。
###### 3.1.3參攷
[​MongoDB Documentation:如何更改oplog的大小​]（ https://docs.mongodb.com/manual/tutorial/change-oplog-size/ ）<br>
[​MongoDB Documentation:如何將單節點轉為複製集​]（ https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/ ）<br>
> **注意**<br>
>如果MongoDB URI未設定w=majority，Tapdata會使用默認的配寘w=1，表示數據寫到primary節點後就返回了。
>如果在數據從primary節點同步到secondary節點前，primary節點發生异常宕機，此時就會發生資料丟失。 囙此建議使用w=majority配寘。
> w=majority表示只有當數據寫到大多數節點後才會返回用戶端正確寫入。
#### **3.2. 作為目標資料庫**
##### **3.2.1基本配置**
-目標端MongoDB支持副本集和分片集羣。
-如果您的目標端MongoDB只有一個節點，您可以將其配寘為單成員的複製集，以開啟oplog功能。
-確保為目標MongoDB配寘了足够的資源來處理源資料庫的工作負載。
##### **3.2.2帳戶許可權**
如果目標MongoDB啟用了安全身份驗證，則Tapdata使用的使用者帳戶必須具有以下角色/許可權：
- `clusterMonitor`（數據驗證功能需要使用）
- `readWrite`（作為目標資料庫需要擁有的角色）
  要創建具有以上許可權的用戶，您可以參考以下示例：
```
> use admin
> db.createUser（{
“user”：“johndoe”，
“pwd”：“my_password”，
“roles”：[
{
“role”：“clusterMonitor”，
“db”：“admin”
}，
{
“role”：“readWrite”，
“db”：“my_db”
}，
{
“role”：“read”，
“db”：“local”
}
]
}
```
> **注意**：只有MongoDB版本3.2需要local資料庫的讀取許可權。
### **4. 同步MongoDB集羣**
當使用MongoDB集羣作為源庫時，Tapdata會為每個分片創建一個線程，以直接從分片主節點（或次節點）讀取數據。< br>
為提高負載效能，我們認為有必要使用這種多執行緒並行的設計方案。 但是需要注意的是，這種方法的副作用是可能會在源集羣庫中產生孤立檔案。 孤立檔案是當MongoDB發生自動數據遷移所導致的。< br>
要解决此問題，建議在使用MongoDB集羣作為源庫同步前，完成以下任務：<br>
- **停止平衡器**<br>
  有關停止平衡器的詳細說明，請參閱：<br>
  [​MongoDB Documentation:如何停止平衡器​]（ https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/ ）
- **使用cleanOrphan命令，請參閱**<br>
  [​MongoDB Documentation:如何清理孤兒檔案​]（ https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/ ）
### **5. MongoDB TLS/SSL配寘**
- **啟用TLS/SSL**<br>
  請在左側配寘頁的“使用TLS/SSL連接”中選擇“是”項進行配寘<br>
- **設定MongoDB PemKeyFile**<br>
  點擊“選擇檔案”，選擇證書檔案，若證書檔案有密碼保護，則在“私密金鑰密碼”中填入密碼<br>
- **設定CAFile**<br>
  請在左側配寘頁的“驗證服務器證書”中選擇“是”<br>
  然後在下方的“認證授權”中點擊“選擇檔案”<br>