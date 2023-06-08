## **连接配置帮助**
### **1. YashanDB 安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用YashanDB数据库。
### **2. 先决条件（作为目标）**
- 以具有增删改查权限的用户身份登录数据库
#### **2.1 创建用户**
```
CREATE USER username IDENTIFIED BY password;
```
#### **2.1 给用户赋予数据库表增删改查权限**
```
GRANT SELECT ANY TABLE, INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE TO username;
```