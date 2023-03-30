## **連接配寘幫助**

### **1. 先決條件（作為源）**

#### **1.1 填寫團隊名稱**

在您的Coding連結中也可以直觀地獲取，比如：**https://team_name.coding.net/** ，那麼他的團隊名稱就是 team_name。

#### **1.2 從Coding管理頁面獲取訪問權杖Token**

填寫完您的團隊名稱後直接點擊授權按鈕，跳轉到授權按鈕後點擊授權後頁面自動返回

### **1.3 选择项目**

#### **1.4 選擇增量方式**

- 此時此刻，有Coding支持的Webhook形式，也有普通按時輪詢的增量方式。
- 當然，如果您選擇了比較節約處理器效能的WebHook模式，那麼您就需要前往Coding配寘Webhook（點擊 ’生成服務 URL‘ 右側的 ‘生成’，您可以看到一行簡潔明瞭的URL，在此，需要您複製前往Coding粘貼到Webhook配寘頁面的服務URL輸入框）

##### **1.3.1 Webhook**

- 此模式需要在創建任務前配寘好ServiceHook：
- 配寘Webhook的流程如下：

```
 1. 一鍵生成服務URL，並複製到剪切板
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/generate.PNG)

```
 2. 進入您的團隊並選擇對應的項目 
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init.PNG)

```
 3. 進入項目設定後，找到開發者選項 
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/developer.PNG)

```
 4. 找到ServerHook，再找到右上角點的新建ServerHook按鈕並點擊 
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init-webhook.PNG)

```
 5. 進入Webhook配寘，第一步我們選擇Http Webhook後點擊下一步 
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/webhook.PNG)

```
 6. 配寘我們需要的監聽的事件類型 
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/monitor.PNG)

```
 7. 粘貼我們最開始在創建資料來源頁面生成的服務URL到此
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/url.PNG)

- 一鍵前往配寘web Hook： https://tapdata.coding.net/p/testissue/setting/webhook

##### **1.3.2 輪詢式**

...

---

特別說明：**創建新的Coding連接，如果選擇WebHook模式，一定要記得前往Coding為此連接節點配寘ServiceHook哦！**

---

### **2. 數據說明**

支持增量輪詢的任何錶在執行增量輪詢時都無法監聽並處理删除事件（所有修改事件都以插入事件處理），如需要具體的事件區分請選擇WebHook增量管道（局限於SaaS平臺，並不是所有錶都支持webHook增量） 

#### **2.1 事項錶-Issues**

事項錶包含全部類型包括需求、反覆運算、任務、史詩以及自定義的類型。
其增量管道在輪詢式下無法準確知道增量事件，統一作為新增事件處理。

#### **2.2 迭代表-Iterations**

迭代表包含所有反覆運算。
受限於Coding的OpenAPI，其輪詢式增量採取從頭覆蓋，意味著任務開始後監控上顯示的增量事件數存在誤差，但不會造成真實數據誤差。

#### **2.3 項目成員錶-ProjectMembers**

此錶包含當前選中的項目下的全部項目成員。