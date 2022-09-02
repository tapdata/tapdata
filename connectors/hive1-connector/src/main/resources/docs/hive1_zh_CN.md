## **连接配置帮助**

### **1. Hive1 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 Hive 数据库。

### **2. 限制说明**

Tapdata 系统当前版本 Hive 仅支持作为目标。

### **3. 支持版本**

Hive3.1.2

### **4. 配置说明**

#### 数据源配置

- Host/IP
- Port
- 数据库名名称
- 账户、密码

#### 开启 CDC 配置

Hive 中 行级操作 update 和 delete 是属于事务操作，所以需要在 Hive 中开启事务操作，修改 `hive-site.xml` 中配置项为如下配置，修改完成后重启生效。

```

<property>
    <name>hive.support.concurrency</name>
    <value>true</value>
</property>
<property>
    <name>hive.enforce.bucketing</name>
    <value>true</value>
</property>
<property>
    <name>hive.exec.dynamic.partition.mode</name>
    <value>nonstrict</value>
</property>
<property>
    <name>hive.txn.manager</name>
    <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
</property>
<property>
    <name>hive.compactor.initiator.on</name>
    <value>true</value>
</property>
<property>
    <name>hive.compactor.worker.threads</name>
    <value>1</value>
</property>
<property>
    <name>hive.in.test</name>
    <value>true</value>
</property>
```

### **5. 连接测试项**

- 检测 host/IP 和 port
- 检查数据库名称
- 检查账号和密码
