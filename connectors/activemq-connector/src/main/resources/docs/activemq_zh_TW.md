## **連接配置說明**

### **1. ACTIVEMQ配置說明**

- 隊列名稱為空，默認加載所有隊列； 如果需要指定，可以用逗號分隔。
- MQ連接串（BrokerUrl）格式：tcp://[host]:[port]。

### **2.使用限制**

>- 僅支持JSON Object字串的消息格式（如`{“id”：1，“name”：“張三”}`），後續會補充JSONBytes，XML等格式。
>- 可以不用提前創建好隊列
>- PDK框架限制且topic管道不能很好的支持全量，topic暫時不可用。

### **3.識別資料類型**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- STRING（長度200以下）
- TEXT