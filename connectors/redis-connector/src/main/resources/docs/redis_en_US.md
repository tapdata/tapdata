## **连接配置帮助**
### **1. REDIS 安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用 Redis 数据库。
### **2. 限制说明**
Tapdata系统当前版本 Redis 仅支持作为目标，支持的数据源的类型为：Oracle、MySQL、MongoDB、PostgreSQL、SQL Server。

|     源端     |  目标端  | 支持情况 |
|:----------:|:-----:|:----:|
|   Oracle   | Redis |  支持  |
|   MySQL    | Redis |  支持  |
|  MongoDB   | Redis |  支持  |
| PostgreSQL | Redis |  支持  |
| SQL Server | Redis |  支持  |

### **3. 支持版本**
Redis 3.3
### **4. 配置说明**
- Host/IP
- Port
- 数据库名
- 哨兵地址
> **特别说明**<br>
> Redis 的密码不是必填项，但是如果您要配置的 Redis 数据库有密码，而您未在Tapdata中配置密码的话，检测会不通过。
>

### **5. 连接测试项**
- 检测host/IP 和 port
- 检查账号和密码

### **6. Redis target data structure description**
First, there are two types of values: Json and Text:
```
Json is to convert Record to Json string;
Text will combine the corresponding values with specific connectors according to the order of the fields. If the content also has this specific character, it will be enclosed with escape characters
```
- String
>Selecting a String type key value will tile it. You need to enter a key expression, such as prefix_${_id}_suffix, the value corresponding to the key is the whole record.
- List
>There are two ways to select a List type:<br>
>1、 The entire table record can be stored as a key, each value of the List corresponds to a record, and the first record can be set as the header (separated by a comma)<br>
>2、 The entire table records can be grouped by key expressions composed of certain fields and then tiled into multiple List key values;
- Hash
>There are two ways to select a hash type:<br>
>1、 The entire table record can be stored as a key, and each key of Hash is processed as a String type<br>
>2、 The entire table records are tiled into multiple key values. The hash corresponding to each key is a record, and each field corresponds to its own value;

The final product boundary description:<br>
1. Due to the diversity of Redis storage modes, the multi key mode does not support structure cleaning in order not to delete data by mistake<br>
2. If the field selected in the key expression may not include the before event of the source, and may not support the Update or Delete event, please select the key expression carefully<br>
3. Finally, if you use single key storage, you should pay special attention to the Redis single key size limit of 512M.