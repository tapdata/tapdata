###  Write on the front 
 If you are interested, you can go to the OpenAPI document and WebHook document provided by ZoHo Desk to learn more about them: 

- OpenAPI Doc：[https://desk.zoho.com.cn/support/APIDocument.do#Introduction](https://desk.zoho.com.cn/support/APIDocument.do#Introduction)
- WebHook Doc：[https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction](https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction)
-  Workflow Configuration Document ：[https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html](https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html)

 Of course, you can also browse the following content to quickly start the configuration process of the ZoHo Desk data source. 

---

### 1. Attribute Description 

1. Organization ID (org ID): your data source organization, you need to manually enter ZoHo Desk to obtain and configure here; 

2. Client ID: The client ID needs the user to go to ZoHo Desk to obtain it manually, copy and paste it here;

3. Client Secret: The client password is obtained in the same way as the client ID code. You can also see the client password when you obtain the client ID code. After entering the client ID code and client password, you can enter the application generation code; 

4. Application generation code: The application generation code needs to be used together with the client ID code and client machine password to obtain the OpenAPI access key and key refresh token. 

5. Connection Mode: The connection mode is available for users to select. The default is the normal document mode, and the options are the normal document mode and CSV mode (not provided yet). 

6. Incremental mode: limited to ZoHo's OpenAPI. ZoHo Desk data sources only support the WebHook incremental mode. See the following description for details. 

7. Service URL: Service URL is used to configure WebHook. You need to copy and paste the service URL generated here to the WebHook configuration item in ZoHo Desk. See the following description for the specific configuration process. 

---

### 2. Configuration Steps 
#### 2.1 Basic configuration 

1.Get * * Organization ID * *: Enter your ZoHo Desk, click Setting in the upper right corner, click the API menu under the developer space, and slide to the bottom. You will see a title "Zoho Service Communication (ZSC) Key". There is an organization ID field below this form. Copy the organization ID here. 

2.Enter Api Console, click the ADD CLIENT button on the upper right corner, and select Self Client;
 - Click the link to enter the API Console: [https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/)

3.Click Client Secret in the menu bar to obtain the Client ID and Client Secret;

4.Next, get the Generate Code, enter the Scope, and enter the complete scope to facilitate the API to obtain data:

```
Desk.tickets.ALL,Desk.search.READ,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
```

 You can also try to open the following links to go to the official documents and splice the appropriate scopes yourself. Remember to separate them with English symbols and commas: 
[https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes](https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes)

5.Select a Time Duration, including 3minutes, 5minutes, 7minutes, and 10minutes. This option means that you need to go back to the TapData Create Connection page to obtain the access token and refresh the token in this time. 

6.After clicking the Create button, you need to manually select the associated project, which is the portal in ZoHo. The selected portal is the next data source. 

7.After the Generate Code is generated, please go back to the TapData creation connection page to obtain the Token within the time period of time duration configuration. After that, you may want to obtain the Generate Code again according to the above steps. 

#### 2.2 WebHook Configuration 

 After configuring webHook, you can realize real-time update of data. 

一：Global configuration of WebHook 

1. First, you need to click the Generate Service URL button to generate the service URL corresponding to the data source. ZoHoDesk will communicate the update event to you according to this URL; 

2. Second, you need to open your ZoHoDesk, enter the Setting panel in the upper right corner, select the developer space, and enter WebHook. Select New webHook in. You need to manually enter the webHook name and paste the service URL generated in the previous step into the URL input box to be notified. Select and add events you need to pay attention to. 

3. After clicking Save, the WebHook takes effect. 

二：Workflow configuration Webhook 

1. If the global WebHook you configured is invalid, you may need to try to configure such a WebHook in the workflow; 

2. Step 1: enter the Setting panel, find the "Automation" option, select the "Workflow" menu, then select the "Rules" menu, and click the right side to create a new rule; 

3. Step 2: create a new rule, select the module corresponding to the rule, give the rule a loud name, and click Next; 

4. Step 3: Select the execution time (I think it should be called the execution event. The Chinese translation of ZoHoDesk is not accurate). This is to select the operation that triggers this workflow; 

5. Step 4: Select the criteria, which can be left unchecked. This option is to filter out specific events. If necessary, you can select and set them. Click Next; 

6. Step 5: Select an operation. What we need to configure here is WebHook. You need to select all operations in the upper left corner of the selected header, click the "+" in the upper right corner of the header, and select Send Cliq Notification under external operations; 

7. Step 6: Edit your operation name, and then paste the service URL generated when you created the ZoHo data source with TapData into the input box corresponding to the InComing WebHook URL; 

8. Step 7: After editing the Notification Message, click Save. This completes the configuration of a workflow. 

---

### 3. Table Description 
1.Tickets： Work Order Form 。

2.Departments： Sectoral table 。

3.Products： Product List 。

4.OrganizationFields： Custom Attribute Field Table 。

5.Contracts： Contract Form 。

......

---

### 4. matters needing attention 

- When configuring the ZoHo data source, you need to generate the service URL and configure it in the ZoHo Desk, otherwise the incremental event will not take effect; 

- When copying a ZoHo data source, you also need to configure a new WebHook for it in the ZoHo Desk, because the service URL is unique for each ZoHo data source. 

- Please do not use the same set of Client IDs and Client Secrets to create too many ZoHo data sources. Because the access keys generated by the same set of Clients are limited, to prevent unnecessary losses caused by OpenAPI flow limiting measures in the process of full replication. Only the ZoHo Desk provides customers with so few OpenAPI access connections, even if you are a noble VIP. 

- In the incremental WebHook mode, if you delete a work order or other data monitored by WebHook on the ZoHo Desk, you will receive an update event about updating the IsDel field according to the rules of the ZoHo Desk. However, if you reset and restart this task at this time, the record you deleted last time will not be obtained by the full read operation, because ZoHo Open Api will not provide records with IsDel field of TRUE.

- As for the service URL, the ZoHo WebHook configuration requires that you ensure that your service URL is open on port 80 or 443, similar to http://xxx.xx.xxx:80/xxxxxxxx , or https://xxx.xx.xxx:443/xxxxxx 。 Therefore, you need to send and receive the data pushed to you by ZoHo Desk on port 80 or port 443.