## **连接配置帮助**
###  **1. MONGODB安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用MongoDB数据库。
> **注意**：MongoDB 作为源端连接时，必须是副本集。
#### **2. 支持版本**
MongoDB 3.2、3.4、3.6、4.0、4.2

>**注意**：<br>
>由于 Tapdata 数据同步目前是基于 MongoDB 的 Change Stream 支持对多表合并的操作，而 MongoDB 官方是从 4.0 版本开始支持 Change Stream 的，因此，请尽量保证源端数据库和目标端数据库都是 4.0 及以上版本。

###  **3. 先决条件**
#### **3.1 作为源数据库**
##### **3.1.1 基本配置**
- 源端 MongoDB 支持副本集和分片集群。
- 如果源端 MongoDB 只有一个节点，您可以将其配置为单成员的复制集，以开启 oplog 功能。
- 您应该配置足够的 oplog 空间。 我们建议至少足以容纳 24 小时的 oplog。

##### **3.1.2 帐户权限**
如果源端 MongoDB 启用了安全身份验证，则 Tapdata 用于连接源端 MongoDB 的用户帐户必须具有以下内置角色：
```
clusterMonitor（读取 oplog ）
readAnyDatabase
```
要创建具有上述权限的用户，您可以参考以下示例：
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
如果您不希望授予` readAnyDatabase `角色，则还可以向特定的数据库以及 local 和 config 数据库赋予读取权限。例如：
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
请注意，只有 MongoDB 版本 3.2 需要 local 数据库的读取权限。

> **重要事项**<br>
> 对于集群分片，您必须在每个分片主节点上创建适当的用户权限。 这是由于MongoDB的安全架构设计。
> 当登录到每个单独的分片时，分片服务器不会向config数据库获取用户权限。 相反，它将使用其本地用户数据库进行身份验证和授权。

######  3.1.3 参考
[​MongoDB Documentation: 如何更改oplog的大小​](https://docs.mongodb.com/manual/tutorial/change-oplog-size/)<br>
[​MongoDB Documentation: 如何将单节点转为复制集​](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/)<br>

> **注意**<br>
> 如果 MongoDB URI 未设置 w=majority ，Tapdata 会使用默认的配置w=1，表示数据写到 primary 节点后就返回了。
> 如果在数据从 primary 节点同步到 secondary 节点前，primary 节点发生异常宕机，此时就会发生数据丢失。因此建议使用 w=majority 配置。
> w=majority表示只有当数据写到大多数节点后才会返回客户端正确写入。
#### **3.2. 作为目标数据库**
#####  **3.2.1 基本配置**
- 目标端 MongoDB 支持副本集和分片集群。
- 如果您的目标端 MongoDB 只有一个节点，您可以将其配置为单成员的复制集，以开启 oplog 功能。
- 确保为目标 MongoDB 配置了足够的资源来处理源数据库的工作负载。

#####  **3.2.2 帐户权限**
如果目标 MongoDB 启用了安全身份验证，则 Tapdata 使用的用户帐户必须具有以下角色 / 权限：
- `clusterMonitor`（数据验证功能需要使用）
- `readWrite`（作为目标数据库需要拥有的角色）
要创建具有以上权限的用户，您可以参考以下示例：
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
> **注意**：只有 MongoDB 版本 3.2 需要 local 数据库的读取权限。

### **4. 同步 MongoDB 集群**
当使用 MongoDB 集群作为源库时，Tapdata 会为每个分片创建一个线程，以直接从分片主节点（或次节点）读取数据。<br>
为提高负载性能，我们认为有必要使用这种多线程并行的设计方案。但是需要注意的是，这种方法的副作用是可能会在源集群库中产生孤立文档。孤立文档是当 MongoDB 发生自动数据迁移所导致的。<br>
要解决此问题，建议在使用 MongoDB 集群作为源库同步前，完成以下任务：<br>
- **停止平衡器**<br>
有关停止平衡器的详细说明，请参阅:<br>
[​MongoDB Documentation: 如何停止平衡器​](https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/)
- **使用cleanOrphan命令,请参阅**<br>
[​MongoDB Documentation: 如何清理孤儿文档​](https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/)

### **5. MongoDB TLS/SSL配置**
- **启用TLS/SSL**<br>
请在左侧配置页的 “使用TLS/SSL连接”中选择“是”项进行配置<br>
- **设置MongoDB PemKeyFile**<br>
点击“选择文件”，选择证书文件，若证书文件有密码保护，则在“私钥密码”中填入密码<br>
- **设置CAFile**<br>
请在左侧配置页的 “验证服务器证书”中选择“是”<br>
然后在下方的“认证授权”中点击“选择文件”<br>

