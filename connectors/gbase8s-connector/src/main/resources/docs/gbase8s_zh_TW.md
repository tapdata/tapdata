## **連接配寘幫助**

### **1. GBase 8s資料庫說明**
官網地址： http://www.gbase8s.com/home.php <br>

GBase 8s資料庫是一款基於informix研發的資料庫，保留了大部分原生的語法、特性及欄位類型，並且引入了大量oracle的優勢特性。<br>

詳細說明可以參見Informix資料及GBase 8s的官方網站
### **2.試用版Docker安裝**
```
docker pull liaosnet/gbase8s:3.3.0_ 2_ amd64
docker run -itd -p 9088:9088 liaosnet/gbase8s:3.3.0_ 2_ amd64
```
### **3.支持版本**
現時GBase 8a向外開放的所有版本

### **4.資料庫特殊性提示（作為目標）**
- GBase 8s滿足事務的支持，需要開啟日誌備份，否則會報錯：Transactions not supported
  （開啟命令：ontape -s–U dbname）。
- GBase 8s可以通過額外連接參數的配寘（delimident=y）設定錶名大小寫敏感，否則錶名使用大寫時會報錯。