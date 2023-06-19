## **connection configuration description**
### **1. REDIS Installation instructions.**
Please follow the instructions below to ensure successful addition and usage of the Redis database in Tapdata.
### **2. Limitation explanation**
The current version of the Tapdata system supports Redis only as a target, meaning it can be used as a destination for data synchronization. The supported data source types for Tapdata include Oracle、MySQL、MongoDB、PostgreSQL、SQL Server.

### **3. Supported versions**
Redis 2.8 - 7.0

### **4. Configuration instructions**
- Host/IP
- Port
- Database Name
- Sentinel name
> **Special instructions**<br>
> Redis password is not a mandatory field in Tapdata. However, if the Redis database you are configuring has a password set and you have not provided the password in Tapdata configuration, the connection test will fail.
>

### **5. Connection test item**
- Test the Host/IP and port.
- Check the account and password.

### **6. Redis target data structure description**
First, there are two types of values: Json and Text:
```
Json is to convert Record to Json string;
Text will combine the corresponding values with specific connectors according to the order of the fields. If the content also has this specific character, it will be enclosed with escape characters
```
- String
>Selecting a String type key value will tile it. You need to enter a key expression, such as prefix_${_id}_suffix, the value corresponding to the key is the whole record.
- List
>There are two ways to select a List type:<br>
>1、 The entire table record can be stored as a key, each value of the List corresponds to a record, and the first record can be set as the header (separated by a comma)<br>
>2、 The entire table records can be grouped by key expressions composed of certain fields and then tiled into multiple List key values;
- Hash
>There are two ways to select a hash type:<br>
>1、 The entire table record can be stored as a key, and each key of Hash is processed as a String type<br>
>2、 The entire table records are tiled into multiple key values. The hash corresponding to each key is a record, and each field corresponds to its own value;

The final product boundary description:<br>
1. Due to the diversity of Redis storage modes, the multi key mode does not support structure cleaning in order not to delete data by mistake<br>
2. If the field selected in the key expression may not include the before event of the source, and may not support the Update or Delete event, please select the key expression carefully<br>
3. Finally, if you use single key storage, you should pay special attention to the Redis single key size limit of 512M.
