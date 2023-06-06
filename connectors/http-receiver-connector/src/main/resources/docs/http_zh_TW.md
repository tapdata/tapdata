# HttpReceiverConnector

## 1 配寘須知

- 錶名稱

- 服務URL

- 數據處理腳本

### 1.1 錶名稱

    錶名稱建議不要使用特殊字元，諸如 ：！@#￥%……&*（）...

### 1.2 服務URL

    服務URL，使用此服務URL到協力廠商平臺支持推送消息的模塊配寘消息推送；
    此數據來源能力基於服務URL是否配寘成功，請您有必要在協力廠商平臺將服務URL配寘好； 
    配寘好後，協力廠商品臺即可根據此服務URL推送消息到任務中，每個協力廠商平臺都有各自對應的消息推送體系，詳細的配寘過程請參照協力廠商平臺的配寘規則。

### 1.3 數據處理腳本

    因為每個協力廠商平臺的消息推送體系都有各自的規則，推送過來的數據也是各有千秋，囙此需要您根據您的需求來使用此腳本靈活取用對應的數據； 
    囙此，數據處理腳本是用來處理協力廠商平臺推送過來的消息，從消息中取出對應需要的數據並以指定規則返回。 
    例如：

    （1）某平臺以WebHook推送過來一個事件： 

```json
{
  "eventType": "ADD_MESSAGE",
  "time": 1256467862232000,
  "message": {
    "title": "This is sample message",
    "context": "Sample message for everyone.",
    "sender": "Zhang San",
    "to": "Li Si",
    "time": 1256467862232000
  },
  "sender": {
    "name": "Zhang San",
    "id": "12354-5664-2130-45-460",
    "dept": "OpenDept"
  }
}
```

    （2）如果對上訴消息的需求是我們僅需要message中的數據，囙此，數據處理腳本可以是：

```js
function handleEvent(eventData) {
    let eventType = eventData.eventType;
    //判斷事件類型，轉換為統一事件類型標識（i：新增，u：修改，d：删除） 
    let type = "i";
    switch(eventType){
        case "ADD_MESSAGE" : type = "i";break;
        case "UPDATE_MESSAGE": type = "u";break;
        case "DELETE_MESSAGE": type = "d";break;
    }
    return {
        "before": eventData.message, //事件發生前的數據，删除類型的事件此值是必填
        "after": eventData.message,  //事件發生後的結果，新增、修改類型的事件此值為必填 
        "opType": type,              //事件的類型，i：新增，u：修改，d：删除 
        "time": eventData.time       //事件發生的時間點，值類型為事件戳 
    }
}
```

    （3）經過數據處理腳本，您將得到如下數據，並在任務中以下數據為入庫的最終數據：

```json
{
  "title": "This is sample message",
  "context": "Sample message for everyone.",
  "sender": "Zhang San",
  "to": "Li Si",
  "time": 1256467862232000
}
```

## 2 關於試運行

    1. 您需要保證您在協力廠商平臺使用這裡的服務URL配寘且已配寘好了一個有效的消息推送服務。 

    2. 您點擊試運行前可以看到歷史消息中的某一條或指定條數，且可使用這些數據進行試運行，並觀察試運行結果； 

    3. 若您有是在協力廠商平臺初次配寘好消息推送服務，那麼您可能存在沒有歷史消息數據的情况，此時，您可以前往協力廠商平臺通過在平臺操作相應的數據來觸發協力廠商平臺的消息推送，此時在消息服務配寘有效的情况下您可以在歷史數據中獲取相應的數據用於試運行；   
    
    4. 當然，如果您已知或者可以通過一些管道獲取協力廠商平臺推送過來的消息數據的結果，您不妨直接手動構建相應的消息數據來試運行您的數據處理腳本，以此來驗證您的數據處理腳本是否符合您的預期。