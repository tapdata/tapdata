## **連接配置幫助**
### **1. OpenGauss安裝說明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用OpenGauss数据库。
### **2. 支持版本**
OpenGauss3.0.0+
### **3. CDC原理和支持**
```
地址：xxxxx
端口：xxxx
数据库：postgres(OpenGauss自动生成)
模型：public
账号：tapdata
密码：tapdata
日志插件：PGOUTPUT (保持默认选项，暂未支持增量功能)
```
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

#### **4.2 權限**
##### **4.2.1 作為源**
- **初始化**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```

##### **4.2.2 作為目標**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **注意**：以上只是基本權限的設置，實際場景可能更加複雜

#### **4.使用最後更新時間戳的方式進行增量同步**
##### **4.3.1 名詞解釋**
**schema**：中文為模型，pgsql一共有3級目錄，庫->模型->表，以下命令中<schema>字符，需要填入表所在的模型名稱
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
