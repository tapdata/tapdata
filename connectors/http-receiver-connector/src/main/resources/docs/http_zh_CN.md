# HttpReceiverConnector

## 1 配置须知

- 表名称
  
- 服务URL
  
- 数据处理脚本

### 1.1 表名称

    表名称建议不要使用特殊字符，诸如：！@#￥%……&*（）...

### 1.2 服务URL

    服务URL，使用此服务URL到第三方平台支持推送消息的模块配置消息推送；
    此数据源能力基于服务URL是否配置成功，请您有必要在第三方平台将服务URL配置好；
    配置好后，第三方品台即可根据此服务URL推送消息到任务中，每个第三方平台都有各自对应的消息推送体系，详细的配置过程请参照第三方平台的配置规则。

### 1.3 数据处理脚本

    因为每个第三方平台的消息推送体系都有各自的规则，推送过来的数据也是各有千秋，因此需要您根据您的需求来使用此脚本灵活取用对应的数据；
    因此，数据处理脚本是用来处理第三方平台推送过来的消息，从消息中取出对应需要的数据并以指定规则返回。
    例如：

    （1）某平台以WebHook推送过来一个事件：

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

    （2）如果对上诉消息的需求是我们仅需要message中的数据，因此，数据处理脚本可以是：

```js
function handleEvent(eventData) {
    let eventType = eventData.eventType;
    //判断事件类型，转换为统一事件类型标识（i：新增，u：修改，d：删除）
    let type = "i";
    switch(eventType){
        case "ADD_MESSAGE" : type = "i";break;
        case "UPDATE_MESSAGE": type = "u";break;
        case "DELETE_MESSAGE": type = "d";break;
    }
    return {
        "before": eventData.message, //事件发生前的数据，删除类型的事件此值是必填
        "after": eventData.message,  //事件发生后的结果，新增、修改类型的事件此值为必填
        "opType": type,              //事件的类型，i：新增，u：修改，d：删除
        "time": eventData.time       //事件发生的时间点，值类型为事件戳
    }
}
```

    （3）经过数据处理脚本，您将得到如下数据，并在任务中以下数据为入库的最终数据：

```json
{
  "title": "This is sample message",
  "context": "Sample message for everyone.",
  "sender": "Zhang San",
  "to": "Li Si",
  "time": 1256467862232000
}
```

## 2 关于试运行

    1. 您需要保证您在第三方平台使用这里的服务URL配置且已配置好了一个有效的消息推送服务。

    2. 您点击试运行前可以看到历史消息中的某一条或指定条数，且可使用这些数据进行试运行，并观察试运行结果；

    3. 若您有是在第三方平台初次配置好消息推送服务，那么您可能存在没有历史消息数据的情况，此时，您可以前往第三方平台通过在平台操作相应的数据来触发第三方平台的消息推送，此时在消息服务配置有效的情况下您可以在历史数据中获取相应的数据用于试运行；   
    
    4. 当然，如果您已知或者可以通过一些方式获取第三方平台推送过来的消息数据的结果，您不妨直接手动构建相应的消息数据来试运行您的数据处理脚本，以此来验证您的数据处理脚本是否符合您的预期。