## **连接配置帮助**
### **1. REDIS 安装说明**
请遵循以下说明以确保在 IKAS 中成功添加和使用 Redis 数据库。
### **2. 限制说明**
IKAS系统当前版本 Redis 仅支持作为目标，支持的数据源的类型为：Oracle、MySQL、MongoDB、PostgreSQL、SQL Server。

|源端|目标端|支持情况|
|:-----------:|:-----------:|:-----------:|
Oracle| Redis |支持
MySQL| Redis |支持
MongoDB| Redis |支持
PostgreSQL| Redis |支持
SQL Server | Redis |支持

### **3. 支持版本**
Redis 3.3
### **4. 配置说明**
- Host/IP
- Port
- 数据库名
- 哨兵地址
> **特别说明**<br>
> Redis 的密码不是必填项，但是如果您要配置的 Redis 数据库有密码，而您未在IKAS中配置密码的话，检测会不通过。
>

### **5. 连接测试项**
- 检测host/IP 和 port
- 检查账号和密码