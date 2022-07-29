## **连接配置说明**

### **1. ACTIVEMQ 配置说明**

- 队列名称为空，默认加载所有队列；如果需要指定，可以用逗号分隔。
- MQ连接串（BrokerUrl）格式：tcp://[host]:[port]。

### **2. 使用限制**

> - 仅支持 JSON Object 字符串的消息格式 (如 `{"id":1, "name": "张三"}`)，后续会补充JSONBytes，XML等格式。
> - 可以不用提前创建好队列
> - PDK框架限制且topic方式不能很好的支持全量，topic暂时不可用。

### **3. 识别数据类型**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- STRING（长度200以下）
- TEXT