## **连接配置说明**

### **1. RABBITMQ 配置说明**

- 队列名称为空，默认加载所有队列；如果需要指定，可以用逗号分隔。
- API端口是指RABBITMQ默认HTTP的API调用端口，默认是8090。（全量加载队列时需要指定）
- 虚拟主机需要匹配账号来使用，符合账号的权限目录，默认是"/"。

### **2. 使用限制**

> - 仅支持 JSON Object 字符串的消息格式 (如 `{"id":1, "name": "张三"}`)，后续会补充JSONBytes，XML等格式。
> - 可以不用提前创建好队列
> - PDK框架限制目前按照default exchange的direct模式，路由功能未支持。

### **3. 识别数据类型**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- STRING（长度200以下）
- TEXT