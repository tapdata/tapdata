## Create Lark data source (target)

You need to go to https://open.feishu.cn/app Find the corresponding application and find it in the ***voucher and basic information*** of the application:

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/findApp.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/appIdAndSecret.PNG)

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
    "phoneOrEmail": "{{phoneOrEmail}}",
    "type": "text",
    "content": "{\"text\":\"Hello! This is lark message! \"}"
  }
]
```
其中：

- type contain ***text*** | ***post*** | ***image*** | ***interactive*** | ***share_chat*** | ***share_user*** | ***audio*** | ***media*** | ***file*** | ***sticker***，default is ***text***。
  For specific message types, see the description on the official document: [https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6)

- phoneOrEmail is the mobile phone number or email address of the message receiver, APP obtains the user's open_id through this field to send a message to the specified user.

1. You need to use the user's registered mobile phone number to send the specified message to this person;

2. You need to ensure that the user of the current mobile phone number exists in the visible range of this application. If it is not in the visible range of the current application version, you cannot send messages to this user. If necessary, you can view the visible range of the latest version in the application version management and release, and create a new version and add this user to the visible range.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/version.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/rang.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/createdVersion.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/FeiShu/doc/modifyRang.PNG)