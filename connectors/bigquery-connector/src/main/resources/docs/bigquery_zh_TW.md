### 寫在前面
 如果您感興趣的話，不妨前往Google提供的檔案，詳細瞭解全部內容 ：

- 說明檔案：[https://cloud.google.com/docs](https://cloud.google.com/docs)
- 操作檔案：[https://cloud.google.com/bigquery/docs/](https://cloud.google.com/bigquery/docs/)
- 创建合管理服務帳號：[https://cloud.google.com/iam/docs/creating-managing-service-accounts](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

 當然您也可以瀏覽以下內容，快速上手BigQuery資料來源的配寘流程。 

---
### 1.内容說明

1.服務帳號：您需要手動前往BigQuery控制台設定規則並創建服務帳號，作為數據訪問憑據；

2.數据集ID：需要您確認BigQuery資料來源對應的數据集並輸入；

---

### 2.配寘步驟

#### 2.1基礎配寘

獲取**服務帳號**：

- 1.請前往BigQuery控制台，進入憑據管理操作介面：[https://console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials)

- 2.如果您已經配寘過相應的服務帳號（***請直接跳過2-6這些步驟直接從第7步開始***），您此刻需要新建一個服務帳號。 點擊功能表列中的**CREATE CREDENTIAL**選項：

  ![](../img/serviceAccount1.png)

- 3.選擇**Service Account**，進行服務帳號的創建：

  ![](../img/serviceAccount2.png)

- 4.分別填寫服務帳號的基本資訊：

  ![](../img/serviceAccount3.png)

- 5.將此服務帳戶關聯到項目，並配寘其存取權限規則，我們這裡需要選擇BigQuery下的BigQuery Admin許可權；

  ![](../img/serviceAccount4.png)
  
  ![](../img/serviceAccount5.png)

- 6.配寘完成後，點擊創建。 我們會回到Credentital頁面，可以在Service Account表格中看到我們剛剛創建好的服務帳號：

 ![](../img/serviceAccount6.png)

- 7.點擊這個創建好的Service account，進入Service account. 此時我們開始配寘訪問秘鑰，也就是我們創建資料來源是需要用到的關鍵資訊。 我們選擇Key是選項，點擊Add key。 創建一個新的key；

 ![](../img/serviceAccount7.png)

- 8.點擊創建，選擇JSON格式的秘鑰。 保存到本地後，打開JSON檔案，複製全部內容到Tapdata創建連接頁面，將複製到的內容粘貼到服務帳號文本域中即可；

 ![](../img/serviceAccount8.png)

獲取**數据集ID**

- 1.進入BigQuery控制台： [https://console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)

- 2.可以從介面，直接獲取數据集ID，如下圖所示，依次看到的層級關係為項目ID->數据集ID->資料表ID：

 ![](../img/tableSetId.png)