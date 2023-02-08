### 寫在前面
如果您感興趣的話，不妨前往ZoHo提供的OpenAPI文檔文檔，詳細了解全部內容：

- OpenAPI文檔：[https://www.zoho.com.cn/crm/help/developer/api/overview.html](https://www.zoho.com.cn/crm/help/developer/api/overview.html)

當然您也可以瀏覽以下內容，快速上手ZoHo數據源的配置流程。

---

### 1.屬性說明

1. 客戶端ID碼(Client ID)：客戶端ID碼需要用戶前往ZoHoDesk手動獲取並複制粘貼到此；

2. 客戶端機密碼(Client Secret)：客戶端機密碼與客戶端ID碼獲取方式一致，您獲取客戶端ID碼的同時也可以看到客戶端機密碼，輸入客戶端ID碼和客戶端機密碼後即可輸入應用生成碼；

3. 用生成碼(Generate Code)：應用生成碼需要與客戶端ID碼和客戶端機密碼配合使用，用於獲取OpenAPI訪問秘鑰和秘鑰刷新令牌。

---

### 2.配置步驟
#### 2.1 基礎配置

1. 註冊客戶端：
    1. 訪問 [https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/) Zoho Developer控制台

    2. 點擊添加客戶端ID.

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client-add-client.jpg)

    4. 輸入以下信息

   客戶端名稱 - 您想要在Zoho註冊的應用程序名稱

   客戶端域 - 要在URL中用於標識web頁面的域名

   授權重定向URL - 系統對接無需使用此參數，可自行填寫一個地址。
   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client2.jpg)
    5. 點擊創建。
    6. 您將收到以下證書：

   客戶端ID – 消費者密鑰由連接的應用程序生成。

   客戶端密鑰 – 消費者密鑰產生於連接的應用程序。

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client3.jpg)

    1. 認證請求
        1. 單擊【ADD CLIENT】按鈕，選擇【Self Client】。
        2. 填寫上面獲取到的客戶端ID和客戶端密鑰。
        3. 在Generate Code頁簽填寫【Scope】和【Scope Description】點擊【CREATE】按鈕。

       Scope填寫方式參見[https://www.zoho.com.cn/crm/help/developer/api/oauth-overview.html](https://www.zoho.com.cn/crm/help/developer/api/oauth-overview.html)
       作用域章節。

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-v2-selfclient2.png)
    4. 生成Generated Code

   ![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_generate_code_3.png)
2. 完成連接配置
    1. 在連接配置中填寫Client ID、Client Secret和授予令牌（Generated Code）
    2. 點擊【刷新令牌】按鈕獲取訪問令牌。
    3. 保存連接。
### 3.表說明
1. Leads：線索。

2. Contacts：聯繫人。

3. Accounts：客戶。

4. Potentials：商機。

5. Quotes：報價。