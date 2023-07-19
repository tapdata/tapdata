## **连接配置帮助**
### **1. Sybase 配置说明**

#### 数据库连接与端口

需要正确填写您的数据库链接和端口

#### 账号密码

配置数据源是请填写您的账号密码

#### 为账号配置权限

例如您的账号是：**username**

请在数据库端执行以下SQL:

```sql

sp_displaylogin username
sp_role 'grant', sa_role, username
sp_role 'grant', replication_role, username
sp_role 'grant', sybase_ts_role, username

```

#### 获取字符集

正确的字符集配置可以正常解析中文繁体与中文简体字文本以及一些特殊字符

请通过已下方式来查询字符集设置

- 执行以下SQL获取 ***Run Value*** 字段的值

```sql
sp_configure 'default character set id';
```

- 获取上一步骤时查询到的 ***Run Value***， 执行以下SQL获取对应字符集，
```sql
select name,id from master..syscharsets where id= ${Run Value}
```