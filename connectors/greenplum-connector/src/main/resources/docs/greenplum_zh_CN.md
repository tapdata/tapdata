## **连接配置帮助**
### **1. OpenGauss安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用OpenGauss数据库。
### **2. 支持版本**
OpenGauss3.0.0+
### **3. 连接配置示例**
```
地址：xxxxx
端口：xxxx
数据库：postgres(OpenGauss自动生成)
模型：public
账号：tapdata
密码：tapdata
日志插件：PGOUTPUT (保持默认选项，暂未支持增量功能)
```
### **4. 先决条件**
#### **4.1 修改REPLICA IDENTITY**
该属性决定了当数据发生`UPDATE,DELETE`时，日志记录的字段
- **DEFAULT** - 更新和删除将包含primary key列的现前值
- **NOTHING** - 更新和删除将不包含任何先前值
- **FULL** - 更新和删除将包含所有列的先前值
- **INDEX index name** - 更新和删除事件将包含名为index name的索引定义中包含的列的先前值
如果有多表合并同步的场景，则Tapdata需要调整该属性为FULL
示例
```
alter table '[schema]'.'[table name]' REPLICA IDENTITY FULL`
```
#### **4.2 权限**
##### **4.2.1 作为源**
- **初始化**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
##### **4.2.2 作为目标**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **注意**：以上只是基本权限的设置，实际场景可能更加复杂

#### **4.3 使用最后更新时间戳的方式进行增量同步**
##### **4.3.1 名词解释**
**schema**：中文为模型，pgsql一共有3级目录，库->模型->表，以下命令中<schema>字符，需要填入表所在的模型名称
### **5. 全类型字段支持**
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
- tsvector (增量不支持不报错)
- tsquery (增量不支持不报错)
- oid
- regproc (增量不支持不报错)
- regprocedure (增量不支持不报错)
- regoper (增量不支持不报错)
- regoperator (增量不支持不报错)
- regclass (增量不支持不报错)
- regtype (增量不支持不报错)
- regconfig (增量不支持不报错)
- regdictionary (增量不支持不报错)
