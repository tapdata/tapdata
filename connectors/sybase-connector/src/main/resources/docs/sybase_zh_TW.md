## **連接配寘幫助**
### **1. Sybase 配置說明**

#### 資料庫連接與埠

需要正確填寫您的資料庫連結和埠

#### 帳號密碼

配寘資料來源是請填寫您的帳號密碼

#### 為帳號配寘許可權

例如您的帳號是：**username**

請在資料庫端執行以下SQL:

```sql

sp_displaylogin username
sp_role 'grant', sa_role, username
sp_role 'grant', replication_role, username
sp_role 'grant', sybase_ts_role, username

```

#### 獲取字元集

正確的字元集配寘可以正常解析中文繁體與中文簡體字文字以及一些特殊字元

請通過已下管道來査詢字元集設定

- 執行以下SQL獲取 ***Run Value*** 欄位的值

```sql
sp_configure 'default character set id';
```

- 獲取上一步驟時査詢到的 ***Run Value***， 執行以下SQL獲取對應字元集 
```sql
select name,id from master..syscharsets where id= ${Run Value}
```