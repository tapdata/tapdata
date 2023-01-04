## **連接配置幫助**
### **1. POSTGRESQL安裝說明**
請遵循以下說明以確保在 Tapdata 中成功添加和使用PostgreSQL數據庫。
### **2. 支持版本**
PostgreSQL 9.4、9.5、9.6、10.x、11.x、12版本
### **3. CDC原理和支持**
#### **3.1 CDC原理**
PostgreSQL 的邏輯解碼功能最早出現在9.4版本中，它是一種機制，允許提取提交到事務日誌中的更改，並通過輸出插件以用戶友好的方式處理這些更改。
此輸出插件必須在運行PostgreSQL服務器之前安裝，並與一個複制槽一起啟用，以便客戶端能夠使用更改。
#### **3.2 CDC支持**
- **邏輯解碼**（Logical Decoding）：用於從 WAL 日誌中解析邏輯變更事件
- **複製協議**（Replication Protocol）：提供了消費者實時訂閱（甚至同步訂閱）數據庫變更的機制
- **快照導出**（export snapshot）：允許導出數據庫的一致性快照（pg_export_snapshot）
- **複製槽**（Replication Slot）：用於保存消費者偏移量，跟踪訂閱者進度。
  所以，根據以上，我們需要安裝邏輯解碼器，現有提供的解碼器如下拉框所示

### **4. 先決條件**
#### **4.1 修改REPLICA IDENTITY**
該屬性決定了當數據發生`UPDATE,DELETE`時，日誌記錄的字段
- **DEFAULT** - 更新和刪除將包含primary key列的現前值
- **NOTHING** - 更新和刪除將不包含任何先前值
- **FULL** - 更新和刪除將包含所有列的先前值
- **INDEX index name** - 更新和刪除事件將包含名為index name的索引定義中包含的列的先前值
  如果有多表合併同步的場景，則Tapdata需要調整該屬性為FULL
  示例
```
alter table '[schema]'.'[table name]' REPLICA IDENTITY FULL`
```

#### **4.2 插件安裝**
- [decorderbufs](https://github.com/debezium/postgres-decoderbufs)
- [Protobuf-c 1.2+](https://github.com/protobuf-c/protobuf-c)
- [protobuf ](https://blog.csdn.net/gumingyaotangwei/article/details/78936608)
- [PostGIS 2.1+ ](http://www.postgis.net/)
- [wal2json ](https://github.com/eulerto/wal2json/blob/master/README.md)
- pgoutput(pg 10.0+)

**安裝步驟**<br>
以 wal2json 為例，安裝步驟如下<br>
確保環境變量PATH中包含"/bin"<br>
```
export PATH=$PATH:<postgres安裝路徑>/bin
```
**安裝插件**<br>
```
git clone https://github.com/eulerto/wal2json -b master --single-branch \
&& cd wal2json \
&& USE_PGXS=1 make \
&& USE_PGXS=1 make install \
&& cd .. \
&& rm -rf wal2json
```
安裝插件報錯處理`make`命令執行，遇到類似 `fatal error: [xxx].h: No such file or directory `的異常信息<br>
**原因**：缺少postgresql-server-dev<br>
**解決方案**：安裝postgresql-server-dev，以debian系統為例<br>
```
// 版本號例如:9.4, 9.6等
apt-get install -y postgresql-server-dev-<版本號>
```
**配置文件**<br>
如果你正在使用一個支持的邏輯解碼插件(不能是 pgoutput )，並且它已經安裝，配置服務器在啟動時加載插件:<br>
```
postgresql.conf
shared_preload_libraries = 'decoderbufs,wal2json'
```
配置replication<br>
```
# REPLICATION
wal_level = logical
max_wal_senders = 1 # 大於0即可
max_replication_slots = 1 # 大於0即可
```

#### **4.3 權限**
##### **4.3.1 作為源**
- **初始化**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
- **增量**<br>
  用戶需要有replication login權限，如果不需要日誌增量功能，則可以不設置replication權限
```
CREATE ROLE <rolename> REPLICATION LOGIN;
CREATE USER <username> ROLE <rolename> PASSWORD '<password>';
// or
CREATE USER <username> WITH REPLICATION LOGIN PASSWORD '<password>';
```
配置文件 pg_hba.conf 需要添加如下內容：<br>
```
pg_hba.conf
local   replication     <youruser>                     trust
host    replication     <youruser>  0.0.0.0/32         md5
host    replication     <youruser>  ::1/128            trust
```

##### **4.3.2 作為目標**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **注意**：以上只是基本權限的設置，實際場景可能更加複雜

##### **4.4  測試日誌插件**
> **注意**：以下操作建議在POC環境進行
>連接postgres數據庫，切換至需要同步的數據庫，創建一張測試表
```
-- 假設需要同步的數據庫為postgres，模型為public
\c postgres

create table public.test_decode
(
  uid    integer not null
      constraint users_pk
          primary key,
  name   varchar(50),
  age    integer,
  score  decimal
)
```
可以根據自己情況創建一張測試表<br>
- 創建 slot 連接，以 wal2json 插件為例
```
select * from pg_create_logical_replication_slot('slot_test', 'wal2json')
```
- 創建成功後，對測試表插入一條數據<br>
- 監聽日誌，查看返回結果，是否有剛才插入操作的信息<br>
```
select * from pg_logical_slot_peek_changes('slot_test', null, null)
```
- 成功後，銷毀slot連接，刪除測試表<br>
```
select * from pg_drop_replication_slot('slot_test')
drop table public.test_decode
```
#### **4.5 異常處理**
- **Slot清理**<br>
  如果 tapdata 由於不可控異常（斷電、進程崩潰等），導致cdc中斷，會導致 slot 連接無法正確從 pg 主節點刪除，將一直佔用一個 slot 連接名額，需手動登錄主節點，進行刪除
  查詢slot信息
```
// 查看是否有slot_name以tapdata_cdc_开头的信息
 TABLE pg_replication_slots;
```
- **刪除slot節點**<br>
```
select * from pg_drop_replication_slot('tapdata');
```
- **刪除操作**<br>
  在使用 wal2json 插件解碼時，如果源表沒有主鍵，則無法實現增量同步的刪除操作

#### **4.6 使用最後更新時間戳的方式進行增量同步**
##### **4.6.1 名詞解釋**
**schema**：中文為模型，pgsql一共有3級目錄，庫->模型->表，以下命令中<schema>字符，需要填入表所在的模型名稱
##### **4.6.2 預先準備（該步驟只需要操作一次）**
- **創建公共函數**
  在數據庫中，執行以下命令
```
CREATE OR REPLACE FUNCTION <schema>.update_lastmodified_column()
    RETURNS TRIGGER language plpgsql AS $$
    BEGIN
        NEW.last_update = now();
        RETURN NEW;
    END;
$$;
```
- **創建字段和trigger**
> **注意**：以下操作，每張表需要執行一次
假設需要增加last update的表名為mytable
- **創建last_update字段**
```
alter table <schema>.mytable add column last_udpate timestamp default now();
```
- **創建trigger**
```
create trigger trg_uptime before update on <schema>.mytable for each row execute procedure
    update_lastmodified_column();
```
### **5. 全類型欄位支持**
- smallint
- integer
- bigint
- numeric
- real
- double precision
- character
- character varying
- text
- bytea
- bit
- bit varying
- boolean
- date
- interval
- timestamp
- timestamp with time zone
- point
- line
- lseg
- box
- path
- polygon
- circle
- cidr
- inet
- macaddr
- uuid
- xml
- json
- tsvector (增量不支持不報錯)
- tsquery (增量不支持不報錯)
- oid
- regproc (增量不支持不報錯)
- regprocedure (增量不支持不報錯)
- regoper (增量不支持不報錯)
- regoperator (增量不支持不報錯)
- regclass (增量不支持不報錯)
- regtype (增量不支持不報錯)
- regconfig (增量不支持不報錯)
- regdictionary (增量不支持不報錯)
