## Create Lark data source (target)

You need to go to https://open.feishu.cn/app Find the corresponding application and find it in the ***voucher and basic information*** of the application:

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_0.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_1.PNG)

1. Get ***App ID*** and fill in here.

2. Get***App Secret***and fill in here.

At this time, the FeiShu data source is created successfully!

### Precautions

#### Use Lark to send the message

If you need to send a message using FeiShu:

***The message body structure should be as follows***：
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

- receiveType is used to represent the type of message receiver. The value range is [ user | chat | email | phone ], represent users, groups and default users respectively.
  
- contentType contain ***text*** | ***post*** | ***image*** | ***interactive*** | ***share_chat*** | ***share_user*** | ***audio*** | ***media*** | ***file*** | ***sticker***，default is ***text***.
  For specific message types, see the description on the official document: [https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json)

- receiveId is the ***open_id*** or ***phone*** or ***email*** or ***chat_id*** of the message receiver，APP sends messages to specified users or group chat through this field.

1. You need to use the user's registered mobile phone number to send the specified message to this person;

2. You need to ensure that the user of the current mobile phone number exists in the visible range of this application. If it is not in the visible range of the current application version, you cannot send messages to this user. If necessary, you can view the visible range of the latest version in the application version management and release, and create a new version and add this user to the visible range.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_2.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_3.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_4.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_6.PNG)