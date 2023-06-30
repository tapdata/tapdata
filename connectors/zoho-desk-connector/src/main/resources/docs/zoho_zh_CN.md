### 写在前面
如果您感兴趣的话，不妨前往ZoHo Desk提供的OpenAPI文档以及WebHook文档，详细了解全部内容：

- OpenAPI文档：[https://desk.zoho.com.cn/support/APIDocument.do#Introduction](https://desk.zoho.com.cn/support/APIDocument.do#Introduction)
- WebHook文档：[https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction](https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction)
- 工作流配置文档：[https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html](https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html)

当然您也可以浏览以下内容，快速上手ZoHo Desk数据源的配置流程。

---

### 1.属性说明

1. 机构 ID(org ID)：您的数据来源机构，需要您手动进入ZoHo Desk获取并配置到此处；

2. 客户端ID码(Client ID)：客户端ID码需要用户前往ZoHo Desk手动获取并复制粘贴到此；

3. 客户端机密码(Client Secret)：客户端机密码与客户端ID码获取方式一致，您获取客户端ID码的同时也可以看到客户端机密码，输入客户端ID码和客户端机密码后即可输入应用生成码；

4. 应用生成码(Generate Code)：应用生成码需要与客户端ID码和客户端机密码配合使用，用于获取OpenAPI访问秘钥和秘钥刷新令牌。

5. 连接模式：连接模式供用户选择，默认普通文档模式，可选有普通文档模式、CSV模式（暂未提供）。

6. 增量方式：局限于ZoHo的OpenAPI，ZoHo Desk数据源仅支持WebHook增量方式，详细的说明见下方说明。

7. 服务 URL：服务URL是用于配置WebHook，需要您把此处生成的服务URL复制粘贴到ZoHo Desk的WebHook配置项，具体的配置流程见下方说明。

---

### 2.配置步骤
#### 2.1 基础配置
1.获取 **机构 ID**：进入您的ZoHo Desk，点击右上角的Setting, 点击 开发者空间下 的 API 菜单，滑动到底部，您可以看到一个标题“Zoho服务通信（ZSC）密钥”，这个表单下面有 机构ID字段，复制这个机构ID到这里即可。

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_org_id.png)

2.进入Api Console 点击右上角 ADD CLIENT 按钮，选择Self Client;
 - 点击链接进入API Console : [https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/)

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_api_colsole.png)

3.点击菜单栏中的Client Secret 可获取Client ID 和Client Secret;

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_api_client_id.png)

4.接下来再去获取Generate Code,输入Scope,输入完整的scope有利于api获取数据：

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_generate_code.png)

```
Desk.tickets.ALL,Desk.search.READ,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
```

您也可以尝试去打开以下链接前往官方文档自己拼接合适的scope，记得用英文符号逗号分隔：
[https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes](https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes)

5.选择一个Time Duration，可选项为3minutes、5minutes、7minutes、10minutes。这个选项表示您接下来需要在此时间内回到TapData创建连接页面获取访问Token和刷新Token。

6.点击Create按钮后，需要您手动选择关联的项目也就是ZoHo所说的门户，选择的门户就是接下来数据的来源。

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_generate_code_2.png)

7.Generate Code生成后请在Time Duration配置的这段时间内回到TapData创建连接页面一件获取Token，超出时间后或许希望您再次按如上步骤获取Generate Code。

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_generate_code_3.png)

#### 2.2 WebHook配置

配置webHook后，您可以实现数据的时实更新。

一：全局配置WebHook

1. 第一步，您需要点击生成服务URl按钮生成数据源对应的服务URL，ZoHo Desk会根据这个URL来向您传达更新事件；

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_webhook_1.png)

2. 第二步，您需要打开您的ZoHo Desk，进入右上角Setting面板,选择开发者空间，进入WebHook。在选择新建webHook。需要您手动输入webHook名称，并把上一步生成的服务URL粘贴到要通知的URL输入框。选择并新增您需要关注的事件。

![](hhttps://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_webhook_2.png)

3. 点击保存后，WebHook即生效。

![](https://gitee.com/code-on-top/picture-temp/raw/master/picture/zoho_webhook_3.png)

二：工作流配置Webhook

1. 如果您配置的全局WebHook无效的话，或许您需要尝试在工作流配置这样一个WebHook；

2. 第一步，进入Setting面板，找到“自动化”选项，选择“工作流”菜单，再选择“规则”菜单，点击右侧新建规则；

3. 第二步，新建规则，选择规则需要对应的模块，给规则起一个响亮的名称，然后点击下一步；

4. 第三步，选择执行时间（我觉得应该叫执行事件，ZoHoDesk的中文翻译不太准确），这是为了选择触发此工作流的操作；

5. 第四步，选择条件，可选可不选，此选项在于筛选过滤出特定的事件，如果需要的话可以选择并设置，点击下一步；

6. 第五步，选择一个操作，这里我们需要配置的是WebHook，所有您需要在选择表格头的左上角选择所有操作，然后再点击表格头的右上角的" + "号选择并选择外部操作下的Send Cliq Notification;

7. 第六步，编辑您的操作名称，再降您在TapData创建ZoHo数据源时生成的服务URL粘贴到InComing WebHook URL对应得输入框中；

8. 第七步，编辑Notification Message后，点击保存。这样您就完整配置好了一个工作流。

---

### 3.表说明
1. Tickets：工单表。

2. Departments：部门表。

3. Products：产品表。

4. OrganizationFields：自定义属性字段表。

5. Contracts：合同表。

......

---

### 4.注意事项

- 您在配置ZoHo数据源时需要生成服务URL并到ZoHo Desk 中进行对应得配置，否则增量事件将不会生效；

- 您在复制一个ZoHo数据源时同样需要到ZoHo Desk中为其配置新的WebHook，因为服务URL对每个ZoHo数据源来说都是独一无二的。

- 请不要使用同一套Client ID 和 Client Secret 创建过多ZoHo 数据源，因为同一套Client生成的访问秘钥是有限的，以防止全量复制过程中发生OpenAPI限流措施导致您产生不必要的损失，仅仅是ZoHo Desk只为客户提供了少得那么可怜的OpenAPI访问连接数，即便您是尊贵的VIP。

- WebHook增量模式下，如若您在ZoHo Desk上删除了某条工单或者其他被WebHook监听的数据，那么根据ZoHo Desk的规则，您将收到一条关于更新IsDel字段的更新事件。但是您若是此时重置并且重启了这个个任务，你上次删除的记录将不在被全量读取操作获取，因为ZoHo Open Api不会提供IsDel字段为TRUE的记录。

- 关于服务URL，由于ZoHo WebHook的配置要求：你需要保证你服务的URL在80或者443端口开放，类似于http://xxx.xx.xxx:80/xxxxxxxx,或者https://xxx.xx.xxx:443/xxxxxx。因此，需要您在80端口或者443端口收发ZoHo Desk推送给您的数据。