## **Connection configuration help**
### **1.YashanDB installation instructions**
Follow these instructions to ensure that the YashanDB database is successfully added and used in Tapdata.
### **2. Prerequisites (as target)**
- Log in to the database as a user with rights to add, delete, modify, and query
#### **2.1 Creating a User**
```
CREATE USER username IDENTIFIED BY password;
```
#### **2.1 Grant users the rights to add, delete, modify, and query database tables**
```
GRANT SELECT ANY TABLE, INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE TO username;
```