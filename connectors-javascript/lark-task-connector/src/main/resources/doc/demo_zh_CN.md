## 创建飞书任务数据源（目标）

您需要前往 https://open.feishu.cn/app 找到对应的应用，并在应用的 ***凭证与基础信息*** 中找到：

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_0.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_1.PNG)

1. 获取到***App ID***,并填写到此处。

2. 获取到***App Secret***,并填写到此处。

此时，飞书数据源创建成功！

### 注意事项

#### 飞书创建任务

***创建任务必须包含以下字段内容：***：

```json
[
  {
    "richSummary": "这里是任务的标题",
    "richDescription": "这里是任务的描述",
    "time": "这里是任务的截止时间，需要传进来时间戳",
    "collaboratorIds": "这里是任务负责人的电话号码或者邮箱",
    "followerIds": "这里任务关注人的电话号码或者邮箱",
    "title": "这里是任务描述里可加链接的标题，可配合下面的url使用",
    "url": "这里给上面的title加上链接"
  }
]
```

其中：
- cUserIds/fUserIds 为任务所有者/任务关注者的***手机号***或***邮箱***，APP通过这个两个字段给定的用户创建任务或添加关注任务。

- 您需要使用用户的注册手机号或邮箱获取到user_id给其创建任务。

- 您需要保证当前手机号或邮箱的使用者存在于此应用的可见范围，如不在当前应用版本的可见范围，将无法发送消息到这个用户，如有必要，您可在应用版本管理与发布中查看最新版本下的可见范围，并创建新的版本并将此用户添加到可见范围。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_2.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_3.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_4.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_6.PNG)
