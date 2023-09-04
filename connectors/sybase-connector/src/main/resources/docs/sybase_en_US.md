## **Connection Configuration Help**
### **1. Sybase Configuration Description**

#### Database Connection and Port

Please fill in your database link and port correctly

#### Account and Password

To configure the data source, please fill in your account password

#### Configure permissions for the account

For example, your account is: **username**

Please execute the following SQL on the database side:

```sql

sp_displaylogin username
sp_role 'grant', sa_role, username
sp_role 'grant', replication_role, username
sp_role 'grant', sybase_ts_role, username

```

#### Get Character Set

The correct character set configuration can normally parse Chinese traditional and Chinese Ryakuji and some special characters

Please query the character set settings through the following method

- Execute the following SQL to obtain the value of field ***Run Value*** 

```sql
sp_configure 'default character set id';
```

- Obtain the ***Run Value***, found in the previous step, execute the following SQL to obtain the corresponding character set
```sql
select name,id from master..syscharsets where id= ${Run Value}
```