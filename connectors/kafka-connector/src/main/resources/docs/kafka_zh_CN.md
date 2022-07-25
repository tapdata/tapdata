## **连接配置帮助**
### **1. Elastic Search安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用 Elastic Search 数据库。
### **2. 限制说明**
Tapdata系统当前版本 Elastic Search 仅支持作为目标，支持的数据源的类型为：Oracle、MySQL、MongoDB、PostgreSQL、SQL Server。

|源端|目标端|支持情况|
|:-----------:|:-----------:|:-----------:|
Oracle| Elastic Search |支持<br>
MySQL| Elastic Search |支持<br>
MongoDB| Elastic Search |支持<br>
PostgreSQL| Elastic Search |支持<br>
SQL Server | Elastic Search |支持<br>

### **3. 支持版本**
Elastic search 7.6
### **4. 配置说明**
- Host/IP
- Port
- 数据库名
- 集群名
> **特别说明**<br>
> Elastic Search 的密码不是必填项，但是如果您要配置的 Elastic Search 数据库有密码，而您未在Tapdata中配置密码的话，检测会不通过。
>

### **5. 连接测试项**
- 检测host/IP 和 port
- 检查账号和密码