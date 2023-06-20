## **connection configuration description**

### **1. TEengine Installation instructions**

Please follow the instructions below to ensure that the TDengine database is successfully added and used in Tapdata.

### **2. Supported version**
TDengine 3.x

### **3. Connection configuration**
#### **3.1 Port configuration**
Full functionality using REST connection. Please configure port 6041.
#### **3.2 Incremental data functionality explanation**
##### **3.2.1 ncremental data port explanation**
TDengine supports new data subscription, which utilizes the Java connector and requires support for port 6030. Additionally, you need to install the client driver for TDengine.
##### **3.2.2 To install the TDengine client driver, follow these steps：**
https://docs.tdengine.com/develop/connect/

### **4. Supported field types**
```
TIMESTAMP
INT
INT UNSIGNED
BIGINT
BIGINT UNSIGNED
FLOAT
DOUBLE
SMALLINT
SMALLINT UNSIGNED
TINYINT
TINYINT UNSIGNED	
BOOL
NCHAR
BINARY (An alias for VARCHAR, BINARY type fields will be converted to VARCHAR type when creating the table.)
VARCHAR
```
### **5. Common issues**
#### **5.1 Error occurred while modifying column width**
TDengine currently does not support modifying the width of certain column types.

