## **连接配置帮助**

### **1. TEengine 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用TDengine数据库。

### **2. 支持版本**
TDengine 3.x

### **3. 连接配置**
#### **3.1 端口设置**
全量功能使用REST 连接，请配置6041端口。
#### **3.2 增量数据功能说明**
##### **3.2.1 增量数据端口说明**
TDengine支持新增数据订阅，订阅功能使用了java连接器，因此需要支持6030端口。 并且安装客户端驱动。
##### **3.2.2 TDengine客户端驱动安装**
https://docs.taosdata.com/connector/#%E5%AE%89%E8%A3%85%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%A9%B1%E5%8A%A8

### **4. 支持字段类型**
```
TIMESTAMP
INT
INT UNSIGNED
BIGINT
BIGINT UNSIGNED
FLOAT
DOUBLE
SMALLINT
SMALLINT UNSIGNED
TINYINT
TINYINT UNSIGNED	
BOOL
NCHAR
BINARY (VARCHAR的别名，BINARY类型字段在建表是会换成VARCHAR类型)
VARCHAR
```
### **5. 常见问题**
#### **5.1 修改列宽报错**
TDengine暂不支持某些字段类型列宽

