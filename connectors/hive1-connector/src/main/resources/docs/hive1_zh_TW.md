## **連接配置幫助**

### **1. Hive1 安裝說明**

請遵循以下說明以確保在 Tapdata 中成功添加和使用 Hive 數據庫。

### **2. 限製說明**

Tapdata 系統當前版本 Hive 僅支持作為目標。

### **3. 支持版本**

Hive3.1.2

### **4. 配置說明**

#### 數據源配置

- Host/IP
- Port
- 數據庫名名稱
- 賬戶、密碼

#### 開啟 CDC 配置

Hive 中 行級操作 update 和 delete 是屬於事務操作，所以需要在 Hive 中開啟事務操作，修改 `hive-site.xml` 中配置項為如下配置，修改完成後重啟生效。

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

### **5. 連接測試項**

- 檢測 host/IP 和 port
- 檢查數據庫名稱
- 檢查賬號和密碼
