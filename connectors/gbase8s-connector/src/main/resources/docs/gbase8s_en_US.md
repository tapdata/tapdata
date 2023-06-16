## **connection configuration help**

### **1. Gbase 8s database description**
Official website address: http://www.gbase8s.com/home.php  <br>

Gbase 8s database is a database developed based on Informix, which retains most of the native syntax, features and field types, and introduces a large number of advantages of Oracle <br>

For detailed instructions, please refer to Informix materials and the official website of gbase 8s
### **2. Trial docker installation**
```
docker pull liaosnet/gbase8s:3.3.0_ 2_ amd64
docker run -itd -p 9088:9088 liaosnet/gbase8s:3.3.0_ 2_ amd64
```
### **3. supported versions**
At present, all versions of GBase 8s are open to the public

### **4. database particularity prompt (as target)**
- GBase 8s meets the transaction support. You need to enable log backup, otherwise an error will be reported: transactions not supported
(start command: ontape -s – u dbname).
- GBase 8s can set the table name to be case sensitive through the configuration of additional connection parameters (delimiter=y), otherwise an error will be reported when the table name is capitalized