## **连接配置帮助**

### **1. SelectDB Cloud安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用SelectDB Cloud数据库。

### **2. 支持版本**
SelectDB 1.0

###  **3. 先决条件（作为目标）**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
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
