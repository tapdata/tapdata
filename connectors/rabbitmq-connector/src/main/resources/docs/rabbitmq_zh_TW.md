## **連接配置說明**

### **1. RABBITMQ配置說明**

- 隊列名稱為空，默認加載所有隊列； 如果需要指定，可以用逗號分隔。
- API埠是指RABBITMQ默認HTTP的API調用埠，默認是8090。 （全量加載隊列時需要指定）
- 虛擬主機需要匹配帳號來使用，符合帳號的許可權目錄，默認是“/”。

### **2.使用限制**
> - 僅支持JSON Object字串的消息格式（如`{“id”：1，“name”：“張三”}`），後續會補充JSONBytes，XML等格式。
> - 可以不用提前創建好隊列
> - PDK框架限制現時按照default exchange的direct模式，路由功能未支持。

### **3.識別資料類型**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- STRING（長度200以下）
- TEXT