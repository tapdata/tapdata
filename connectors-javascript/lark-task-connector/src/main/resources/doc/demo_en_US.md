## Create Lark data source (target)

You need to go to https://open.feishu.cn/app Find the corresponding application and find it in the ***voucher and basic information*** of the application:

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_0.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_1.PNG)

1. Get ***App ID*** and fill in here.

2. Get***App Secret***and fill in here.

At this time, the FeiShu data source is created successfully!

###Precautions

####Fly book creation quest

***The creation task must contain the following fields：***：

```json
[
  {
    "richSummary": "Here is the title of the task",
    "richDescription": "Here is the task description",
    "time": "Here is the task expiration time, need to pass in the timestamp",
    "collaboratorIds": "Here is the task leader's phone number or email address",
    "followerIds": "Here the task focuses on the person's phone number or email address",
    "title": "Here is the linkable title in the task description, which can be used with the following url",
    "url": "Here's a link to the title above"
  }
]
```

其中：
- cUserIds/fUserIds is the ***mobile phone number***  or ***email address*** of the task owner/task follower. The APP uses these two fields to create a task or add a task.

- You need to use the user's registered phone number or email address to obtain the user_id to create tasks for the user.

- You need to ensure that the user with the current mobile phone number or email address is in the visible range of this application. If not in the visible range of the current application version, you will not be able to send messages to this user. If necessary, you can check the visible range under the latest version in the application version management and publishing, and create a new version and add this user to the visible range.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_2.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_3.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_4.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_6.PNG)