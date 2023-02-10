### 写在前面
如果您感兴趣的话，不妨前往ZoHo提供的OpenAPI文档文档，详细了解全部内容：

- OpenAPI文档：[https://www.zoho.com.cn/crm/help/developer/api/overview.html](https://www.zoho.com.cn/crm/help/developer/api/overview.html)

当然您也可以浏览以下内容，快速上手ZoHo数据源的配置流程。

---

### 1.属性说明

1. 客户端ID码(Client ID)：客户端ID码需要用户前往ZoHoDesk手动获取并复制粘贴到此；

2. 客户端机密码(Client Secret)：客户端机密码与客户端ID码获取方式一致，您获取客户端ID码的同时也可以看到客户端机密码，输入客户端ID码和客户端机密码后即可输入应用生成码；

3. 用生成码(Generate Code)：应用生成码需要与客户端ID码和客户端机密码配合使用，用于获取OpenAPI访问秘钥和秘钥刷新令牌。

---

### 2.配置步骤
#### 2.1 基础配置

1. 注册客户端：
   1. 访问 [https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/) Zoho Developer控制台
   
   2. 点击添加客户端ID.
      
   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client-add-client.jpg)

   4. 输入以下信息
      
   客户端名称 - 您想要在Zoho注册的应用程序名称 

   客户端域 - 要在URL中用于标识web页面的域名

   授权重定向URL - 系统对接无需使用此参数，可自行填写一个地址。
      ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client2.jpg)
    5. 点击创建。
    6. 您将收到以下证书：
   
   客户端ID – 消费者密钥由连接的应用程序生成。

   客户端密钥 – 消费者密钥产生于连接的应用程序。

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client3.jpg)

   1. 认证请求
      1. 单击【ADD CLIENT】按钮，选择【Self Client】。
      2. 填写上面获取到的客户端ID和客户端密钥。
      3. 在Generate Code页签填写【Scope】和【Scope Description】点击【CREATE】按钮。
   
       Scope填写方式参见[https://www.zoho.com.cn/crm/help/developer/api/oauth-overview.html](https://www.zoho.com.cn/crm/help/developer/api/oauth-overview.html)
   作用域章节。
   
   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-v2-selfclient2.png)
      4. 生成Generated Code
   
   ![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_generate_code_3.png)
2. 完成连接配置
   1. 在连接配置中填写Client ID、Client Secret和授予令牌（Generated Code）
   2. 点击【刷新令牌】按钮获取访问令牌。
   3. 保存连接。
### 3.表说明
1. Leads：线索。

2. Contacts：联系人。

3. Accounts：客户。

4. Potentials：商机。

5. Quotes：报价。

