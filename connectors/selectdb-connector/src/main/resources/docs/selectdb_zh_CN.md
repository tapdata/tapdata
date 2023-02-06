## **连接配置帮助**

### **1. SelectDB Cloud安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用SelectDB Cloud数据库。

### **2. 支持版本**
#### **2.1 支持内核版本**
SelectDB Cloud 2.0.13 以上，低于此内核版本，CopyInto功能不可使用，无法同步数据。可在SelectDB Cloud管理控制台查看版本并升级。
#### **2.2 支持连接协议版本**
Mysql5.7，Mysql8.0
###  **3. 先决条件（作为目标）**
#### **3.1 数据库权限**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```

#### **3.2 配置参数示例**
```
连接名称：Tapdata
仓库公网 IP：39.108.5.66
MySQL 协议端口：16604
HTTP 协议端口：42188
数据库：TEST
账号：admin
密码：admin
```
###  **4. 常见错误**

Unknown error 1044
如果权限已经grant了，但是通过tapdata还是无法通过测试连接，可以通过下面的步骤检查并修复
```
SELECT host,user,Grant_priv,Super_priv FROM Doris.user where user='username';
//查看Grant_priv字段的值是否为Y
//如果不是，则执行以下命令
UPDATE Doris.user SET Grant_priv='Y' WHERE user='username';
FLUSH PRIVILEGES;
```

