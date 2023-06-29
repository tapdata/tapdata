## **連接配置說明**
### **1. KAFKA安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用Kafka資料庫。
### **2.使用限制**
> -僅支持JSON Object字串的消息格式（如`{“id”：1，“name”：“張三”}`）
> -提前創建好主題
> - kafka版本2.3.x
    > -如果選擇忽略消費或推送异常，則仍然會記錄這些消息的`offset`，即是後續不會推送這些消息，存在資料丟失風險
    > -消息推送實現為`At least once`，對應的消費端要做好幂等操作
#### **2.1同步模式**
##### **僅全量**
>該模式下Source會從主題的各個分區`earliest offset`開始訂閱消費。 如果之前存在消息消費記錄，則會恢復到之前的`offset`開始消費
##### **僅增量**
>該模式下Source會從主題的各個分區`latest offset`開始訂閱消費。 如果之前存在消息消費記錄，則會恢復到之前的`offset`開始消費
##### **全量+增量**
>該模式下Source會跳過全量同步階段，從增量階段開始。
>
> 1.如果沒有進行過全量同步，則會從主題的各個分區`earliest offset`開始訂閱消費
> 2.否則從主題的各個分區`latest offset`開始訂閱消費。
> 3.如果之前存在消息消費記錄，則會恢復到之前的`offset`開始消費
#### **2.2節點連接**
| source | target |是否可連結|
| ------------- | ------------- | ---------- |
| kafka | elasticsearch |是|
| kafka | redis |是|
| kafka | table |是|
| kafka | collection |是|
| kafka | memory |是|
| elasticsearch | kafka |是|
| table | kafka |是|
| redis | kafka |是|
| collection | kafka |是|
| memory | kafka |是|
##### **2.3數據遷移**
| source | target |是否可連結|
| ---------- | ---------- | ---------- |
| kafka | mysql |是|
| kafka | oracle |是|
| kafka | mongodb |是|
| kafka | db2 |是|
| kafka | postgres |是|
| kafka | mssql |是|
| kafka | Base 8s |是|
| kafka | Sybase ASE |是|
| mysql | kafka |是|
| oracle | kafka |是|
| mongodb | kafka |是|
| db2 | kafka |是|
| postgres | kafka |是|
| Sybase ASE | kafka |是|
| Base 8s | kafka |是|
| mssql | kafka |是|
### **3.配寘**
##### **3.1公共配寘**
|欄位名（UI表單參數名）|類型|是否必填|備註|預設值|校驗| UI表單欄位名稱 | UI表單欄位組件|
| ---------------------- | ------ | -------- | ------------------- | ------ | ---------------------------------------------------------------------------------------- |----------| ------------------------- |
| kafkaBootstrapServers | String |是| Broker地址清單| - | host1:port，host2:port，host3:port（如192.168.1.1:9092192.168.1.2:9092192.168.1.3:9092）| 主機清單     | `<input type=“text”/>` |
| databaseType | String |是|資料庫類型| - |固定值：kafka | 無        | `<input type=“hidden”/>` |
| connection_ type | String |是|連結類型| - |枚舉值：source \| target \ | source_ and_ target |連結類型| `<select />` |
| kafkaPatternTopics | String |是|主題名稱通配運算式，| - |文字長度大於0，小於256 | 主題通配運算式  | `<input type=“text”/>` |
##### **3.2 Source（Kafka Consumer）**
|欄位名（UI表單參數名）|類型|是否必填|備註|預設值|校驗| UI表單欄位名稱| UI表單欄位組件|
| ------------------------ | ------- | -------- | ----------------------------------------------------------------------------------- | ------ | --------------------- | -------------------- | ---------------- |
| kafkaIgnoreInvalidRecord | Boolean |否|是否忽略非JSON Object格式消息，如果是則遇到解析异常會忽略該消息，否則停止拉取消息| false |枚舉值：true \| false |忽略非JSON格式消息| `<select />` |
##### **3.3 Target（Kafka Producer）**
|欄位名（UI表單參數名）|類型|是否必填|備註|預設值|校驗| UI表單欄位名稱| UI表單欄位組件|
| ---------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------- | ---------------- | ---------------- |
| kafkaAcks | String |否| ACK確認機制，“0”：不確認，“1”：僅寫入master分區，“-1”：寫入大多數ISR分區，“all”：寫入所有ISR分區| -1 |枚舉值：“0”\|“1”\|“-1”\|“all”|消息推送ACK | `<select />` |
| kafkaCompressionType | String |否|消息壓縮類型：gzip，snappy，lz4，zstd. 大流量消息開啟壓縮可以提高傳輸效率. | - | 枚舉值：“gzip”\|“snappy”\|“lz4”\|“zstd”|消息推送壓縮管道| `<select />` |
| kafkaIgnorePushError | Boolean |否|是否忽略推送消息异常，如果是則忽略該次推送的消息（存在消息遺失），否則停止推送消息| false |枚舉值：true \| false |消息推送忽略异常| `<select />` |