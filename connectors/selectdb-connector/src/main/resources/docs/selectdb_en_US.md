## **Connection configuration help**

### **1. SelectDB Cloud installation instructions**

Please follow the instructions below to ensure that the SelectDB database is successfully added and used in Tapdata.

### **2. Supported version**

#### **2.1 Supported kernel version**

SelectDB Cloud 2.0.13 or later, the CopyInto function is unavailable and data cannot be synchronized. You can view the
version and upgrade it in the SelectDB Cloud administration console.

#### **2.2 Connection protocol versions are supported**

Mysql5.7，Mysql8.0

### **3. Prerequisites (as a goal)**

#### **3.1 Database Permission**

Grant full permissions to a database:

```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```

For global permissions:

```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```

#### **3.2 Example of configuration parameters**

```
Connection name : Tapdata
Repo IP : 39.108.5.66
MySQL Port : 16604
HTTP Port : 42188
Database : TEST
User : admin
Password : admin
```

### **4. Frequent fault**

Unknown error 1044 If the permissions have been granted but the test connection cannot be passed through tapdata, you
can check and fix it by following the steps below

SELECT host,user,Grant_priv,Super_priv FROM Doris.user where user='username';
//Check whether the value of Grant priv is Y
//If not, run the following command
UPDATE Doris.user SET Grant_priv='Y' WHERE user='username';
FLUSH PRIVILEGES;
```
