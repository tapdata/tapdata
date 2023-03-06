## **Connection configuration help**
###  **1.  MONGODB installation instructions**
Please follow the instructions below to ensure that the MongoDB database is successfully added and used in Tapdata.
> **Note**: MongoDB must be a replica set when it is used as the source connection.
#### **2.  Supported versions**
MongoDB 3.2、3.4、3.6、4.0、4.2
> **Note**:<br>
>Since Tapdata data synchronization is currently based on MongoDB's Change Stream, which supports multi-table merging, and MongoDB officially supports Change Stream from version 4.0, please try to ensure that the source and target databases are both version 4.0 and above.
###  **3.  Prerequisites**
#### **3.1 as the source database**
##### **3.1.1 Basic configuration**
- The source MongoDB supports replica sets and sharding clusters.
- If the source MongoDB has only one node, you can configure it as a single-member replication set to enable the oplog function.
- You should configure enough oplog space. We suggest that it is at least enough to accommodate 24 hours of oplog.
##### **3.1.2 Account permissions**
If security authentication is enabled for the source MongoDB, the user account that Tapdata uses to connect to the source MongoDB must have the following built-in roles:
```
ClusterMonitor (read oplog)
readAnyDatabase
```
To create a user with the above permissions, you can refer to the following example:
```
use admin
db.createUser({
"user" : "johndoe",
"pwd"  : "my_password",
"roles" : [
{
"role" : "clusterMonitor",
"db" : "admin"
},
{
"role" : "readAnyDatabase",
"db" : "admin"
}
]
}
```
If you do not want to grant the 'readAnyDatabase' role, you can also grant read permissions to specific databases and local and config databases. For example:
```
use admin
db.createUser({
"user" : "johndoe",
"pwd"  : "my_password",
"roles" : [
{
"role" : "clusterMonitor",
"db" : "admin"
},
{
"role" : "read",
"db" : "my_db"
}，
{
"role" : "read",
"db" : "local"
},
{
"role" : "read",
"db" : "config"
}
]
}
```
Please note that only MongoDB version 3.2 requires read access to the local database.
> **Important matters**<br>
>For cluster sharding, you must create appropriate user permissions on each sharding master node. This is due to the security architecture design of MongoDB.
>When logging into each separate partition, the partition server will not obtain user permissions from the config database. Instead, it will use its local user database for authentication and authorization.
###### 3.1.3 Reference
     [MongoDB Documentation: How to change the size of oplog]（ https://docs.mongodb.com/manual/tutorial/change-oplog-size/ )<br>
     [MongoDB Documentation: How to convert a single node into a replica set]（ https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/ )<br>
> **Note**<br>
>If MongoDB URI is not set to w=major, Tapdata will use the default configuration of w=1, which means that the data is returned after being written to the primary node.
>If the primary node goes down abnormally before the data is synchronized from the primary node to the secondary node, data loss will occur. Therefore, it is recommended to use w=major configuration.
>W=majority means that the client can only write the data correctly after it is written to most nodes.
#### **3.2.  As target database**
##### **3.2.1 Basic configuration**
- MongoDB on the target side supports replica sets and sharding clusters.
- If your target MongoDB has only one node, you can configure it as a single-member replication set to enable the oplog function.
- Ensure that sufficient resources are configured for the target MongoDB to handle the workload of the source database.
##### **3.2.2 Account permissions**
If the target MongoDB has security authentication enabled, the user account used by Tapdata must have the following roles/permissions:
- 'clusterMonitor' (data validation function needs to be used)
- 'readWrite '(as the role of the target database)
To create a user with the above permissions, you can refer to the following example:
```
> use admin
> db.createUser({
"user" : "johndoe",
"pwd"  : "my_password",
"roles" : [
{
"role" : "clusterMonitor",
"db" : "admin"
},
{
"role" : "readWrite",
"db" : "my_db"
},
{
"role" : "read",
"db" : "local"
}
]
}
```
> **Note**: Only MongoDB version 3.2 requires read access to the local database.
### **4.  Synchronize MongoDB cluster**
When using the MongoDB cluster as the source library, Tapdata will create a thread for each shard to read data directly from the primary node (or secondary node) of the shard< br>
In order to improve the load performance, we think it is necessary to use this multi-thread parallel design scheme. However, it should be noted that the side effect of this method is that isolated documents may be generated in the source cluster library. Orphaned documents are caused by MongoDB's automatic data migration< br>
To solve this problem, it is recommended to complete the following tasks before using MongoDB cluster as source database synchronization:<br>
- **Stop balancer**<br>
For detailed instructions on stopping the balancer, see:<br>
[MongoDB Documentation: How to stop the balancer]（ https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/ )
- **Use the cleanOrphan command, see**<br>
[MongoDB Documentation: How to clean up orphan documents]（ https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/ )
### **5.  MongoDB TLS/SSL configuration**
- **Enable TLS/SSL**<br>
Please select "Yes" in "Connect using TLS/SSL" on the left configuration page to configure<br>
- **Set MongoDB PemKeyFile**<br>
Click "Select File" and select the certificate file. If the certificate file is password protected, fill in the password in "Private Key Password"<br>
- **Set CAFile**<br>
Please select "Yes" in "Validate server certificate" on the left configuration page<br>
Then click "Select File" in "Authentication and Authorization" below<br>