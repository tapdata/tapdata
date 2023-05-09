##創建飛書資料來源（目標）

您需要前往 https://open.feishu.cn/app 找到對應的應用，並在應用的***憑證與基礎資訊***中找到：
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_0.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_1.PNG)

1.獲取到***App ID***，並填寫到此處。

2.獲取到***App Secret***，並填寫到此處。

此時，飛書資料來源創建成功！

###注意事項

####飛書發消息

如您需要使用飛書發消息：

***消息體結構應該如下***：
```json
[
  {
    "receiveType": "{{user | email | phone | chat}}",
    "receiveId": "{{user_open_id | user_email | user_phone | chat_id}}",
    "contentType": "text",
    "content": "{\"text\":\"Hello! This is lark message! \"}"
  }
]
```
其中：

- receiveType用來表示消息接收者類型，取值範圍為[ user | chat | email | phone ], 分別表示用戶、群組，默認用戶。

- contentType包含 ***text*** | ***post*** | ***image*** | ***interactive*** | ***share_chat*** | ***share_user*** | ***audio*** | ***media*** | ***file*** | ***sticker***，默认***text***。
  具體的消息類型可查看官方檔案上的描述：[https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json)

- receiveId為消息接收者的***open_id***或***手機號***或***郵箱***或***群聊的chat_id***，APP通過這個欄位來發送消息到指定的用戶或群聊。

1.您需要使用用戶的注册手機號發送指定消息到此人；

2.您需要保證當前手機號的使用者存在於此應用的可見範圍，如不在當前應用版本的可見範圍，將無法發送消息到這個用戶，如有必要，您可在應用版本管理與發佈中查看最新版本下的可見範圍，並創建新的版本並將此用戶添加到可見範圍。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_2.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_3.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_4.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_6.PNG)