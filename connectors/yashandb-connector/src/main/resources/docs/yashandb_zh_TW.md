## **連接配置幫助**
### **1. YashanDB 安裝說明**
請遵循以下說明以確保在 Tapdata 中成功添加和使用YashanDB數據庫。
### **2. 先決條件（作爲目標）**
- 以具有增刪改查權限的用戶身份登錄數據庫
#### **2.1 創建用戶**
```
CREATE USER username IDENTIFIED BY password;
```
#### **2.1 給用戶賦予數據庫表增刪改查權限**
```
GRANT SELECT ANY TABLE, INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE TO username;
```