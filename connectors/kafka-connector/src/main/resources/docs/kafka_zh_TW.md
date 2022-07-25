## **連接配置幫助**
### **1. Elastic Search安裝說明**
請遵循以下說明以確保在 Tapdata 中成功添加和使用 Elastic Search 數據庫。
### **2. 限制說明**
Tapdata系統當前版本 Elastic Search 僅支持作為目標，支持的數據源的類型為：Oracle、MySQL、MongoDB、PostgreSQL、SQL Server。

|源端|目標端|支持情況|
|:-----------:|:-----------:|:-----------:|
Oracle| Elastic Search |支持<br>
MySQL| Elastic Search |支持<br>
MongoDB| Elastic Search |支持<br>
PostgreSQL| Elastic Search |支持<br>
SQL Server | Elastic Search |支持<br>

### **3. 支持版本**
Elastic search 7.6
### **4. 配置說明**
- Host/IP
- Port
- 數據庫名
- 集群名
> **特別說明**<br>
> Elastic Search 的密碼不是必填項，但是如果您要配置的 Elastic Search 數據庫有密碼，而您未在Tapdata中配置密碼的話，檢測會不通過。
>

### **5. 連接測試項**
- 檢測host/IP 和 port
- 檢查賬號和密碼