## **连接配置帮助**

### **1. Aliyun ADB MySQL 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用Aliyun ADB MySQL数据库。

### **2. 支持版本**
Aliyun ADB MySQL 5.0、5.1、5.5、5.6、5.7、8.x

### **3. 先决条件（作为源）**
#### **3.1 开启 Binlog**
- 必须开启 Aliyun ADB MySQL 的 binlog ，Tapdata 才能正常完成同步工作。
- 级连删除（CASCADE DELETE），这类由数据库产生的删除不会记录在binlog内，所以不被支持。
修改 `$MYSQL_HOME/mysql.cnf `, 例如:
```
server_id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 1
binlog_format     = row
binlog_row_image  = full
```
配置解释：<br>
server-id: 对于 Aliyun ADB MySQL 中的每个服务器和复制客户端必须是唯一的<br>
binlog_format：必须设置为 row 或者 ROW<br>
binlog_row_image：必须设置为 full<br>
expire_logs_days：二进制日志文件保留的天数，到期会自动删除<br>
log_bin：binlog 序列文件的基本名称<br>

#### **3.2 重启 Aliyun ADB MySQL**

```
/etc/inint.d/mysqld restart
```
验证 binlog 已启用，请在 mysql shell 执行以下命令
```
show variables like 'binlog_format';
```
输出的结果中，format value 应该是"ROW"

验证 binlog_row_image 参数的值是否为full:
```
show variables like 'binlog_row_image';
```
输出结果中，binlog_row_image value应该是"FULL"

#### **3.3 创建Aliyun ADB MySQL账号**
Mysql8以后，对密码加密的方式不同，请注意使用对应版本的方式，设置密码，否则会导致无法进行增量同步
##### **3.3.1 5.x版本**
```
create user 'username'@'localhost' identified by 'password';
```
##### **3.3.2 8.x版本**
```
// 创建用户
create user 'username'@'localhost' identified with mysql_native_password by 'password';
// 修改密码
alter user 'username'@'localhost' identified with mysql_native_password by 'password';

```

#### **3.4 给 tapdata 账号授权**
对于某个数据库赋于select权限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
#### **3.5 约束说明**
```
当从Aliyun ADB MySQL同步到其他异构数据库时，如果源Aliyun ADB MySQL存在表级联设置，因该级联触发产生的数据更新和删除不会传递到目标。如需要在目标端构建级联处理能力，可以视目标情况，通过触发器等手段来实现该类型的数据同步。
```

###  **4. 关于更新事件**
AliYun ADB Mysql 更新事件不可更新主键，因此写入需要判断修改前和修改后的主键值是否相同，相同时需要移除主键进行修改，不相同则拆成删除和插入处理