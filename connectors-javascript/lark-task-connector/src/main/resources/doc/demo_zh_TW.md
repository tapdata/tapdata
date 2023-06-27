##創建飛書資料來源（目標）

您需要前往 https://open.feishu.cn/app 找到對應的應用，並在應用的***憑證與基礎資訊***中找到：
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_0.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_1.PNG)

1.獲取到***App ID***，並填寫到此處。

2.獲取到***App Secret***，並填寫到此處。

此時，飛書資料來源創建成功！

###注意事項

####飛書創建任務

***創建任務必須包含以下字段內容：***：

```json
[
  {
    "richSummary": "這裏是任務的標題",
    "richDescription": "這裏是任務的描述",
    "time": "這裏是任務的截止時間，需要傳進來時間戳",
    "collaboratorIds": "這裏是任務負責人的電話號碼或者郵箱",
    "followerIds": "這裏任務關注人的電話號碼或者郵箱",
    "title": "這裏是任務描述裏可加鏈接的標題，可配合下面的url使用",
    "url": "這裏給上面的title加上鍊接"
  }
]
```

其中：
- cUserIds/fUserIds 爲任務所有者/任務關注者的***手機號***或***郵箱***，APP通過這個兩個字段給定的用戶創建任務或添加關注任務。

- 您需要使用用戶的註冊手機號或郵箱獲取到user_id給其創建任務。

- 您需要保證當前手機號或郵箱的使用者存在於此應用的可見範圍，如不在當前應用版本的可見範圍，將無法發送消息到這個用戶，如有必要，您可在應用版本管理與發佈中查看最新版本下的可見範圍，並創建新的版本並將此用戶添加到可見範圍。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_2.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_3.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_4.PNG)

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/lark/step_6.PNG)