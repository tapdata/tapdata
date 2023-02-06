## **連接配置幫助**
### **1. Tablestore 安裝說明**
請遵循以下說明以確保在 Tapdata 中成功添加和使用 Elastic Search 數據庫。
### **2. 限製說明**
Tapdata系統當前版本 Tablestore 僅支持作為目標。

### **3. 支持版本**
Tablestore 5.13.9
### **4. 配置密鑰**
要接入阿裏雲的表格存儲服務，您需要擁有一個有效的訪問密鑰進行簽名認證。目前支持下面三種方式：

#### 阿裏雲賬號的AccessKey ID和AccessKey Secret。創建步驟如下：
* 在阿裏雲官網註冊阿裏雲賬號。
* 創建AccessKey ID和AccessKey Secret。
#### 被授予訪問表格存儲權限RAM用戶的AccessKey ID和AccessKey Secret。創建步驟如下：
* 使用阿裏雲賬號前往訪問控製RAM，創建一個新的RAM用戶或者使用已經存在的RAM用戶。
* 使用阿裏雲賬號授予RAM用戶訪問表格存儲的權限。
#### RAM用戶被授權後，即可使用自己的AccessKey ID和AccessKey Secret訪問。從STS獲取的臨時訪問憑證。獲取步驟如下：
* 應用的服務器通過訪問RAM/STS服務，獲取一個臨時的AccessKey ID、AccessKey Secret和SecurityToken發送給使用方。
* 使用方使用上述臨時密鑰訪問表格存儲服務。

### **5. 註意事項**
* 創建數據表後需要幾秒鐘進行加載，在此期間對該數據表的讀/寫數據操作均會失敗。請等待數據表加載完畢後再進行數據操作。
* 創建數據表時必須指定數據表的主鍵。主鍵包含1個~4個主鍵列，每一個主鍵列都有名稱和類型。
* 不支持清空表數據。