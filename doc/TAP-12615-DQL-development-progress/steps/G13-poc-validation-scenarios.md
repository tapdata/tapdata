# G13 DQL POC 场景测试用例与执行记录

## 1. 文档目的

本文是一套可以从零执行的 DQL POC 测试用例，不是验收大纲。测试人员按照 TC-00 到 TC-13 的顺序执行，每个用例都记录：

1. 具体的页面操作或命令；
2. 需要观察的任务、目标表、异常事件列表或浏览器 Network；
3. 每一步的预期结果；
4. 用例通过和失败的判定。

本用例固定使用 MongoDB 源端 -> MongoDB 目标端、CDC 任务和一张订单表。除登录态、任务 ID、连接 ID、事件 ID 外，不允许临时替换名称或数据，否则后续查询无法直接复用。

> 重要：本用例使用 MongoDB validator 制造稳定的记录级写入失败。它可以证明“异常记录进入 DQL、任务继续处理、修复后可恢复”，但 MongoDB validator 返回的错误在当前分类器中通常是 TARGET_WRITE_ERROR，不能人为标成 POISON_RECORD。只有目标连接器明确返回 PDK SKIPPABLE_DATA 错误码时，才执行本文的严格 POISON_RECORD 分支。

## 2. 固定环境和变量

### 2.1 版本和服务

在 TM 仓库目录执行：

~~~bash
cd /Users/gavinxiao/kit/tapdata/tapdata
git branch --show-current
git log -1 --oneline
~~~

预期：

- 当前分支为 TAP-12615-DLQ-4.22，或当前 POC 约定的包含 DQL 改动的分支；
- TM、Engine、Web 均来自同一套可互通的构建版本；
- Web 可通过 http://localhost:3030 访问，TM API 可通过 http://localhost:3000/api 访问。

本文使用以下固定值：

| 项目 | 固定值 |
|---|---|
| Web | http://localhost:3030 |
| TM API | http://localhost:3000/api |
| 控制面 MongoDB | localhost:27017，由本地 compose 提供 |
| 源 MongoDB | 容器 dql-poc-source，宿主端口 27018 |
| 目标 MongoDB | 容器 dql-poc-target，宿主端口 27019 |
| Engine 连接源/目标时的主机 | host.docker.internal |
| 源数据库/集合 | dql_poc_source.dql_poc_orders |
| 目标数据库/集合 | dql_poc_target.dql_poc_orders |
| 源连接名称 | DQL-POC-SOURCE |
| 目标连接名称 | DQL-POC-TARGET |
| 任务名称 | DQL-POC-MONGO-CDC |
| 任务类型 | cdc / 页面上的“增量（CDC）” |

首次创建任务后记录以下动态值：

~~~text
TASK_ID       = 页面或 GET /api/Task 返回的任务 ID
SOURCE_DS_ID  = DQL-POC-SOURCE 的连接 ID
TARGET_DS_ID  = DQL-POC-TARGET 的连接 ID
ACCESS_TOKEN  = 当前登录 Web 使用的访问令牌（如 API 调试需要）
~~~

## 3. TC-00：启动 TM、Engine、Web 和独立 MongoDB

### 3.1 启动 TapData 本地服务

在终端执行：

~~~bash
cd /Users/gavinxiao/kit/tapdata/tapdata
docker compose -f docker-compose.local.yml up -d mongo tm engine web
docker compose -f docker-compose.local.yml ps
~~~

预期：

- tapdata-mongo、TM、Engine、Web 状态为 Up 或 running；
- TM 的 3000 和 Web 的 3030 端口可用；
- http://localhost:3030 能打开登录页或已登录首页。

如果本机尚未有用于源端和目标端的独立 MongoDB，执行以下命令。只允许在没有同名容器时执行：

~~~bash
docker run -d --name dql-poc-source \
  --add-host=host.docker.internal:host-gateway \
  -p 27018:27017 \
  mongo:6.0 --replSet rs0 --bind_ip_all

docker run -d --name dql-poc-target \
  --add-host=host.docker.internal:host-gateway \
  -p 27019:27017 \
  mongo:6.0 --replSet rs0 --bind_ip_all

docker exec dql-poc-source mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"host.docker.internal:27018"}]})'

docker exec dql-poc-target mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"host.docker.internal:27019"}]})'
~~~

### 3.2 验证 MongoDB 可连接

执行：

~~~bash
mongosh "mongodb://localhost:27018/?directConnection=true" --quiet --eval \
  'printjson({ping:db.adminCommand({ping:1}).ok,state:rs.status().myState})'

mongosh "mongodb://localhost:27019/?directConnection=true" --quiet --eval \
  'printjson({ping:db.adminCommand({ping:1}).ok,state:rs.status().myState})'
~~~

预期：源端和目标端都返回 ping: 1、state: 1。如果 rs.status() 不是主节点状态 1，不要创建任务，先修复副本集连接地址。

TC-00 通过标准：TM、Engine、Web 均已启动；源端 27018、目标端 27019 可以从宿主机访问；Engine 能通过 host.docker.internal:27018/27019 访问两端。

## 4. TC-01：构建源表和目标表

### 4.1 创建源集合

源集合允许 amount 和 eventTime 暂时出现错误类型，这样可以输入格式错误数据。执行：

~~~bash
mongosh "mongodb://localhost:27018/?directConnection=true" --quiet
~~~

在 mongosh 中粘贴：

~~~javascript
use dql_poc_source
db.dropDatabase()

db.createCollection("dql_poc_orders", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "orderNo", "amount", "eventTime", "status", "scenario", "opSeq"],
      properties: {
        _id: { bsonType: "string" },
        orderNo: { bsonType: "string" },
        amount: { bsonType: ["decimal", "string", "double", "int", "long"] },
        eventTime: { bsonType: ["date", "string"] },
        status: { bsonType: "string" },
        scenario: { bsonType: "string" },
        opSeq: { bsonType: "int" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
})

db.dql_poc_orders.createIndex({ orderNo: 1 }, { unique: true })
db.dql_poc_orders.getIndexes()
~~~

预期：集合创建成功；索引至少包括 _id_ 和唯一的 orderNo_1；countDocuments() 返回 0。

### 4.2 创建目标集合

打开另一个终端：

~~~bash
mongosh "mongodb://localhost:27019/?directConnection=true" --quiet
~~~

在 mongosh 中粘贴：

~~~javascript
use dql_poc_target
db.dropDatabase()

db.createCollection("dql_poc_orders", {
  validator: {
    $and: [
      {
        $jsonSchema: {
          bsonType: "object",
          required: ["_id", "orderNo", "amount", "eventTime", "status", "scenario", "opSeq"],
          properties: {
            _id: { bsonType: "string" },
            orderNo: { bsonType: "string" },
            amount: { bsonType: "decimal" },
            eventTime: { bsonType: "date" },
            status: { bsonType: "string" },
            scenario: { bsonType: "string" },
            opSeq: { bsonType: "int" }
          }
        }
      },
      { scenario: { $ne: "POISON" } }
    ]
  },
  validationLevel: "strict",
  validationAction: "error"
})

db.dql_poc_orders.createIndex({ orderNo: 1 }, { unique: true })
db.dql_poc_orders.getIndexes()
~~~

预期：目标集合创建成功；amount 必须为 Decimal128、eventTime 必须为 BSON Date；scenario=POISON 的记录会被拒绝；集合初始为空。

> 这里的 POISON 是业务测试标记，不等于 DQL 的 POISON_RECORD 枚举。当前 MongoDB validator 失败按实际响应通常归类为 TARGET_WRITE_ERROR。

TC-01 通过标准：两端集合结构、字段类型和唯一索引都符合上面定义，且两端记录数为 0。

## 5. TC-02：创建源端和目标端连接

### 5.1 创建源连接

在 http://localhost:3030 登录后：

1. 进入“连接管理”，点击“新建连接”；
2. 数据库类型选择“MongoDB”；
3. 连接名称填写 DQL-POC-SOURCE；
4. Host 填写 host.docker.internal，Port 填写 27018；
5. Database 填写 dql_poc_source；
6. 用户名、密码留空，认证关闭；
7. 点击“测试连接”，成功后点击“保存”；
8. 记录连接 ID 为 SOURCE_DS_ID。

预期：测试连接成功，连接列表出现 DQL-POC-SOURCE 且状态可用。不要把 Host 填成 localhost，因为对 Engine 容器而言 localhost 是 Engine 自身。

### 5.2 创建目标连接

重复上述操作，填写：

| 页面字段 | 值 |
|---|---|
| 连接名称 | DQL-POC-TARGET |
| Host | host.docker.internal |
| Port | 27019 |
| Database | dql_poc_target |
| 认证 | 关闭 |

预期：测试连接成功，连接列表出现 DQL-POC-TARGET，记录连接 ID 为 TARGET_DS_ID。

### 5.3 API 核对

打开浏览器开发者工具 Network，刷新连接列表，确认出现：

~~~text
GET /api/Connections
~~~

预期响应能找到两个刚保存的连接。若页面显示成功但 Network 没有真实请求，或响应中没有这两个连接，TC-02 失败。

## 6. TC-03：创建、配置并启动 CDC 任务

### 6.1 在 Web 创建任务

在 Web 中：

1. 进入“数据开发”，点击“新建任务”；
2. 任务名称填写 DQL-POC-MONGO-CDC；
3. 任务类型选择“增量（CDC）”；
4. 源连接选择 DQL-POC-SOURCE；
5. 源库选择 dql_poc_source，源表/集合选择 dql_poc_orders；
6. 目标连接选择 DQL-POC-TARGET；
7. 目标库选择 dql_poc_target，目标表/集合填写 dql_poc_orders；
8. 目标端已有数据处理选择“保留目标端”（keepData）；
9. 如果有写入模式，选择 upsert 或当前版本支持的幂等写入模式；
10. 保存任务但先不要启动，记录 TASK_ID。

预期：任务保存成功，Network 中出现 POST /api/Task 或对应保存请求，进入任务编辑页。

### 6.2 添加转换脚本节点

在任务 DAG 中添加“JavaScript Processor”（或同义脚本处理节点），内容填写：

~~~javascript
function process(record) {
  var after = record.after || {};
  if (after.scenario === "SCRIPT_FAIL" ||
      after.scenario === "SEQ_ORDER") {
    throw new Error("DQL_POC_SCRIPT_FAIL");
  }
  return record;
}
~~~

点击“测试脚本”或“保存节点”。预期：普通测试记录通过，脚本节点出现在源端到目标端的 DAG 中。

### 6.3 配置记录级异常处理

打开高级设置或错误处理设置：

1. 异常数据处理选择“跳过异常数据”，对应后端 SkipData；
2. 异常数量限制模式选择“按数量跳过”，对应 SkipByLimit；
3. 限制数量填写 100，比例字段（如果有）填写 100；
4. 保存任务。

保存后必须在 Network 的任务响应中核对：

~~~json
"skipErrorEvent": {
  "errorMode": "SkipData",
  "limitMode": "SkipByLimit",
  "limit": 100
}
~~~

不要把页面上仅用于展示的 DLQ 文案直接当作后端枚举值。若响应为 DLQ、为空，或 GET 任务不是 SkipData，停止执行并记录为前后端配置契约错误。

### 6.4 设置重试参数

如果页面可配置任务重试，设置重试间隔 5 秒、最大重试时间 1 分钟。页面没有字段时，用已登录的 ACCESS_TOKEN 执行：

~~~bash
curl -sS -X PATCH \
  "http://localhost:3000/api/Task/$TASK_ID?access_token=$ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "retryIntervalSecond": 5,
    "maxRetryTimeMinute": 1,
    "skipErrorEvent": {
      "errorMode": "SkipData",
      "limitMode": "SkipByLimit",
      "limit": 100,
      "rate": 100
    }
  }'
~~~

预期：返回成功状态码，且响应中的重试参数和 skipErrorEvent.errorMode=SkipData 正确。

### 6.5 启动前核对和启动

执行：

~~~bash
curl -sS \
  "http://localhost:3000/api/Task/$TASK_ID?access_token=$ACCESS_TOKEN" \
  -H 'Accept: application/json'
~~~

任务启动前必须满足：类型为 cdc（某些响应使用 syncType）；源/目标连接和表正确；skipErrorEvent.errorMode=SkipData；DAG 有脚本节点；重试为 5 秒/1 分钟。

点击“启动任务”，或执行：

~~~bash
curl -sS -X PUT \
  "http://localhost:3000/api/Task/start/$TASK_ID?access_token=$ACCESS_TOKEN"
~~~

预期：出现 PUT /api/Task/start/{TASK_ID} 或对应页面请求；任务状态变为运行中；任务进入 CDC 监听，不因源端暂无数据而结束。

任务不能启动、状态不是运行中、或 SkipData 没有真正保存，TC-03 失败。后续事件结果不能用于证明 DQL。

## 7. TC-04：正常记录基线

### 7.1 插入原始数据

在源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertMany([
  {
    _id: "N-001", orderNo: "ORD-N-001",
    amount: Decimal128.fromString("10.00"),
    eventTime: ISODate("2026-08-28T09:00:00.001Z"),
    status: "CREATED", scenario: "NORMAL", opSeq: NumberInt(1)
  },
  {
    _id: "N-002", orderNo: "ORD-N-002",
    amount: Decimal128.fromString("20.00"),
    eventTime: ISODate("2026-08-28T09:00:00.002Z"),
    status: "CREATED", scenario: "NORMAL", opSeq: NumberInt(1)
  }
])
~~~

预期：返回 acknowledged: true 和 insertedIds，源端记录数增加 2。

### 7.2 检查目标端和 DQL

等待不超过 30 秒，在目标端执行：

~~~javascript
use dql_poc_target
db.dql_poc_orders.find(
  { _id: { $in: ["N-001", "N-002"] } },
  { _id: 1, orderNo: 1, amount: 1, eventTime: 1, status: 1, scenario: 1, opSeq: 1 }
).sort({ _id: 1 }).toArray()
~~~

预期：目标有两条记录，字段值一致，amount 为 Decimal128、eventTime 为 BSON Date，任务仍运行中；异常事件 summary 和列表没有新增这两条记录。

TC-04 通过标准：两条正常记录成功 CDC 到目标，任务不停止，DQL 事件数不增加。

## 8. TC-05：短暂目标故障只触发任务级重试

### 8.1 制造故障和写入

先在源端写入：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertOne({
  _id: "T-001", orderNo: "ORD-T-001",
  amount: Decimal128.fromString("30.00"),
  eventTime: ISODate("2026-08-28T09:01:00.001Z"),
  status: "CREATED", scenario: "TARGET_SHORT_OUTAGE", opSeq: NumberInt(1)
})
~~~

立即在另一个终端执行：

~~~bash
docker stop dql-poc-target
sleep 10
docker start dql-poc-target
~~~

如果 T-001 已经在停机前到达目标，再写入 T-002，内容同上但 _id/orderNo 改为 T-002/ORD-T-002，金额为 31.00。

### 8.2 观察任务级重试

在任务详情查看状态和日志，或执行：

~~~bash
docker logs --since 2m tapdata-engine 2>&1 | rg 'retry|connection|timeout|T-001|T-002'
~~~

预期：

- 目标不可用期间出现连接失败、超时或任务级 retry 日志；
- 任务不会因为一条记录被置为错误；
- 目标恢复后，T-001/T-002 最终写入目标；
- DQL 异常事件数量不增加；
- 不会把目标整体不可用期间的积压数据批量写入 DQL。

TC-05 通过标准：短暂故障走任务级重试，正常记录最终到达目标，DQL 事件数不变。若每条记录都生成 DQL，TC-05 失败。

## 9. TC-06：持续共享故障导致任务错误，不批量生成 DQL

### 9.1 停止目标并写入积压

执行：

~~~bash
docker stop dql-poc-target
~~~

目标停止后 5 秒内在源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertMany([
  {
    _id: "S-001", orderNo: "ORD-S-001", amount: Decimal128.fromString("40.00"),
    eventTime: ISODate("2026-08-28T09:02:00.001Z"),
    status: "CREATED", scenario: "TARGET_LONG_OUTAGE", opSeq: NumberInt(1)
  },
  {
    _id: "S-002", orderNo: "ORD-S-002", amount: Decimal128.fromString("41.00"),
    eventTime: ISODate("2026-08-28T09:02:00.002Z"),
    status: "CREATED", scenario: "TARGET_LONG_OUTAGE", opSeq: NumberInt(1)
  },
  {
    _id: "S-003", orderNo: "ORD-S-003", amount: Decimal128.fromString("42.00"),
    eventTime: ISODate("2026-08-28T09:02:00.003Z"),
    status: "CREATED", scenario: "TARGET_LONG_OUTAGE", opSeq: NumberInt(1)
  }
])
~~~

保持目标停止至少 90 秒，覆盖本任务的 1 分钟最大重试时间。

### 9.2 观察任务和 DQL

预期：

- 任务经历重试后进入错误或失败状态；
- 日志记录目标共享故障和重试耗尽；
- 配置的告警渠道收到任务错误告警；
- S-001、S-002、S-003 不会因为共享目标故障被批量创建为 DQL 事件；
- 异常事件总数不应按积压数量增加。

这里的“任务保持运行”不适用于持续共享故障；它只适用于短暂故障和记录级异常。

### 9.3 恢复任务

执行：

~~~bash
docker start dql-poc-target
mongosh "mongodb://localhost:27019/?directConnection=true" --quiet --eval \
  'printjson({ping:db.adminCommand({ping:1}).ok,state:rs.status().myState})'
curl -sS -X PUT \
  "http://localhost:3000/api/Task/start/$TASK_ID?access_token=$ACCESS_TOKEN"
~~~

预期：Mongo 返回 ping:1/state:1；任务重新运行；CDC 从可用位点继续；目标最终出现三条 S-*。若 oplog 已超出保留范围，应记录为环境容量问题，不能当作 DQL 恢复通过。

## 10. TC-07：格式错误记录进入 DQL，正常记录继续到达

### 10.1 插入格式错误和正常记录

源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertMany([
  {
    _id: "M-001", orderNo: "ORD-M-001", amount: "not-a-number",
    eventTime: ISODate("2026-08-28T09:03:00.001Z"),
    status: "CREATED", scenario: "MALFORMED_AMOUNT", opSeq: NumberInt(1)
  },
  {
    _id: "N-003", orderNo: "ORD-N-003", amount: Decimal128.fromString("50.00"),
    eventTime: ISODate("2026-08-28T09:03:00.002Z"),
    status: "CREATED", scenario: "NORMAL_AFTER_MALFORMED", opSeq: NumberInt(1)
  }
])
~~~

预期：源端两条都写入成功。

### 10.2 检查事件

在“高级功能 / 异常事件”等待列表刷新，或在 Network 确认：

~~~text
GET /api/dql-events/summary
GET /api/dql-events?skip=0&limit=20&order=-failedAt
~~~

打开 M-001 详情。预期：

- M-001 生成一条 DQL 事件；
- 状态初始为 PENDING 或 NOT_REPROCESSABLE，以接口实际返回为准；
- 当前 MongoDB validator 路径预期 errorType=TARGET_WRITE_ERROR；
- 事件原始数据含 _id=M-001、orderNo=ORD-M-001、amount="not-a-number"；
- 任务继续运行；
- N-003 正常写入目标。

### 10.3 可选的严格 MALFORMED_RECORD 分支

如果 DAG 节点列表有“字段类型转换”节点：

1. 在脚本节点前增加该节点；
2. 将 amount 配置为 Decimal128/数字类型，转换失败策略为抛出；
3. 保存并重启任务；
4. 插入 amount="still-not-a-number" 的 M-002；
5. 查看事件详情。

只有 Engine/PDK 抛出被分类器识别的数值转换异常时，类型才应为 MALFORMED_RECORD。如果实际是 TARGET_WRITE_ERROR 或 UNKNOWN_RECORD_ERROR，按实际响应记录，不修改结果。

## 11. TC-08：转换脚本失败，修复后恢复事件

### 11.1 插入脚本失败记录和正常记录

保持 TC-03 的脚本，在源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertMany([
  {
    _id: "X-001", orderNo: "ORD-X-001", amount: Decimal128.fromString("60.00"),
    eventTime: ISODate("2026-08-28T09:04:00.001Z"),
    status: "CREATED", scenario: "SCRIPT_FAIL", opSeq: NumberInt(1)
  },
  {
    _id: "N-004", orderNo: "ORD-N-004", amount: Decimal128.fromString("61.00"),
    eventTime: ISODate("2026-08-28T09:04:00.002Z"),
    status: "CREATED", scenario: "NORMAL_AFTER_SCRIPT_FAIL", opSeq: NumberInt(1)
  }
])
~~~

### 11.2 预期结果

- X-001 生成一条事件；
- 事件类型为 TRANSFORM_ERROR，详情含 DQL_POC_SCRIPT_FAIL；
- 原始数据可查看；
- 任务保持运行；
- N-004 正常到达目标。

如果 X-001 使整个任务停止，说明脚本异常没有走记录级跳过路径，TC-08 失败。

### 11.3 修复脚本和恢复

将脚本改为：

~~~javascript
function process(record) {
  return record;
}
~~~

保存脚本并确认任务运行，然后在 Web 中：

1. 刷新异常事件列表；
2. 打开 X-001 详情；
3. 确认状态为 PENDING 或 RECOVERY_FAILED；
4. 点击“预览”，确认只选择 X-001 和 TASK_ID；
5. 点击“提交恢复”；
6. 等待状态从 REPROCESSING 变为 RECOVERED；
7. 在目标端查询 _id="X-001"。

预期请求：

~~~text
POST /api/dql-events/recovery/preview
POST /api/dql-events/recovery
GET  /api/dql-events/{EVENT_ID}
~~~

预期结果：预览通过后才能提交；恢复成功后事件为 RECOVERED，恢复尝试数增加 1；目标出现 X-001；任务仍运行。若点击恢复没有这些请求，或只改变前端列表，TC-08 失败。

## 12. TC-09：稳定失败的业务 Poison Record

### 12.1 插入 Poison 和正常记录

目标集合当前禁止 scenario=POISON。源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.insertMany([
  {
    _id: "P-001", orderNo: "ORD-P-001", amount: Decimal128.fromString("70.00"),
    eventTime: ISODate("2026-08-28T09:05:00.001Z"),
    status: "CREATED", scenario: "POISON", opSeq: NumberInt(1)
  },
  {
    _id: "N-005", orderNo: "ORD-N-005", amount: Decimal128.fromString("71.00"),
    eventTime: ISODate("2026-08-28T09:05:00.002Z"),
    status: "CREATED", scenario: "NORMAL_AFTER_POISON", opSeq: NumberInt(1)
  }
])
~~~

### 12.2 预期结果

- P-001 进入一条事件；
- 当前 MongoDB 路径预期 errorType=TARGET_WRITE_ERROR；
- 详情能看到原始 scenario=POISON；
- 任务继续运行；
- N-005 正常到达目标；
- 刷新详情不会产生重复事件。

### 12.3 修复业务规则并恢复

在目标端移除 scenario != POISON 条件，保留字段类型校验：

~~~javascript
use dql_poc_target
db.runCommand({
  collMod: "dql_poc_orders",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "orderNo", "amount", "eventTime", "status", "scenario", "opSeq"],
      properties: {
        _id: { bsonType: "string" },
        orderNo: { bsonType: "string" },
        amount: { bsonType: "decimal" },
        eventTime: { bsonType: "date" },
        status: { bsonType: "string" },
        scenario: { bsonType: "string" },
        opSeq: { bsonType: "int" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
})
~~~

在 Web 对 P-001 执行“预览 -> 提交恢复”。预期事件变为 RECOVERED，目标出现 P-001，任务保持运行。

### 12.4 严格 POISON_RECORD 分支

只有目标连接器明确返回 PDK SKIPPABLE_DATA 时才执行：

1. 使用该连接器规定的可稳定复现的 skippable 数据；
2. 记录原始输入和连接器错误码；
3. 重复本节 12.2；
4. 只有实际接口返回 errorType=POISON_RECORD 才判定严格枚举通过。

如果只能得到 MongoDB 文档校验失败，不得把它改写成 POISON_RECORD。报告中分别记录“业务 Poison 行为验证”和“严格 POISON_RECORD 枚举未验证/不适用”。

## 13. TC-10：同一业务键 Insert、Update、Delete 顺序和最终状态

### 13.1 产生三条失败事件

把脚本恢复为 TC-03 的版本，使 scenario=SEQ_ORDER 抛出异常。在源端连续执行，不要在三条命令之间手工等待：

~~~javascript
use dql_poc_source

db.dql_poc_orders.insertOne({
  _id: "Q-001", orderNo: "ORD-Q-001", amount: Decimal128.fromString("80.00"),
  eventTime: ISODate("2026-08-28T09:06:00.001Z"),
  status: "CREATED", scenario: "SEQ_ORDER", opSeq: NumberInt(1)
})

db.dql_poc_orders.updateOne(
  { _id: "Q-001" },
  { $set: {
      amount: Decimal128.fromString("85.00"),
      status: "PAID",
      eventTime: ISODate("2026-08-28T09:06:00.002Z"),
      opSeq: NumberInt(2),
      scenario: "SEQ_ORDER"
  }}
)

db.dql_poc_orders.deleteOne({ _id: "Q-001" })
~~~

预期：源端最终查不到 Q-001；CDC 中存在 Insert、Update、Delete 三个变更。

### 13.2 检查并按服务端顺序恢复

在异常事件列表按任务 TASK_ID、关键字 ORD-Q-001 查询，打开三条详情并记录 EVENT_ID。预期三条事件属于同一任务、同一源表/目标表，DML 类型分别为 Insert、Update、Delete。

然后：

1. 选择这三条事件；
2. 点击“预览”；
3. 检查返回的 orderedEvents；
4. 确认顺序为 opSeq=1 -> 2 -> 3，或由事件时间和服务端稳定排序得到同样结果；
5. 确认三条事件属于同一个 TASK_ID；
6. 提交恢复；
7. 轮询详情直到三条都为 RECOVERED。

预期：

- Preview 拒绝跨任务混选；
- 预览顺序来自服务端，不以页面当前数组顺序为准；
- Recovery 按 Insert、Update、Delete 执行；
- 目标最终查不到 Q-001，证明 Delete 没有在 Insert 前执行，也没有只恢复最后一条。

## 14. TC-11：逐项核查 Web 操作是否真正调用 API

此用例验证列表、筛选、刷新、详情、预览、恢复不是前端假数据或只改本地状态。

### 14.1 打开异常事件页

开发者工具 Network 勾选 Preserve log，进入“高级功能 / 异常事件”。预期至少看到：

~~~text
GET /api/dql-events/summary
GET /api/dql-events?skip=0&limit=20&order=-failedAt
~~~

### 14.2 验证筛选

每次清空上一个条件后执行下一个：

| 页面操作 | 预期请求 | 预期结果 |
|---|---|---|
| 关键字输入 X-001 并查询 | GET /api/dql-events?...&keyword=X-001 | 只显示匹配事件 |
| 选择任务 DQL-POC-MONGO-CDC | GET /api/dql-events?...&taskId=TASK_ID | 只显示该任务事件 |
| 选择 TRANSFORM_ERROR | GET /api/dql-events?...&errorType=TRANSFORM_ERROR | 只显示脚本错误 |
| 选择 DML Update | 请求包含 dmlType | 只显示 Update 事件 |
| 点击重置 | 重新请求默认列表和 summary | 条件清空、恢复默认 |

每次都必须出现新的 HTTP 请求，或出现防抖后的新请求。没有请求、URL 没带筛选条件、或只是前端静态过滤，判定 Web 适配失败。

### 14.3 验证刷新

1. 记下当前列表第一条事件；
2. 在源端制造新的脚本失败记录 X-002；
3. 点击刷新；
4. 查看 Network 和列表。

预期：重新调用 summary 和列表接口；X-002 出现在列表；loading 在请求结束后消失；不能只排序或复用旧响应。

### 14.4 验证详情、预览、恢复

点击事件行和恢复操作，预期请求：

~~~text
GET  /api/dql-events/{EVENT_ID}
POST /api/dql-events/recovery/preview
POST /api/dql-events/recovery
~~~

预期：详情对应服务端的 EVENT_ID；Preview 失败时不发送 Recovery；Recovery 成功后详情轮询并显示 RECOVERED；列表刷新后与详情一致。

## 15. TC-12：错误路径和权限路径

### 15.1 跨任务恢复

准备两个不同任务的事件，或在 API 调试工具中把两个 TASK_ID 的事件一起放入 Preview。

预期：Preview 返回 4xx/业务校验失败，指出事件必须属于同一任务；页面不允许提交。

### 15.2 不可恢复事件

选择状态为 NOT_REPROCESSABLE 的事件（如果当前版本能产生），打开详情并点击恢复。

预期：详情可查看，但恢复按钮禁用，不发送 Recovery 请求。

### 15.3 重复提交

对 REPROCESSING 或 RECOVERED 的事件再次点击恢复。

预期：按钮禁用，或 API 返回幂等成功/冲突；不能产生第二个并行恢复批次。

### 15.4 非法事件 ID

请求：

~~~text
GET /api/dql-events/not-exist-event-id
~~~

预期：返回 404 或统一业务错误；页面显示可理解的提示，不显示空白成功页。

## 16. TC-13：最终对账和清理

### 16.1 目标数据对账

源端执行：

~~~javascript
use dql_poc_source
db.dql_poc_orders.countDocuments()
db.dql_poc_orders.find({
  _id: { $in: ["N-001", "N-002", "T-001", "T-002", "S-001", "S-002",
               "S-003", "N-003", "N-004", "N-005"] }
}).count()
~~~

目标端执行：

~~~javascript
use dql_poc_target
db.dql_poc_orders.countDocuments()
db.dql_poc_orders.find({
  _id: { $in: ["N-001", "N-002", "T-001", "T-002", "S-001", "S-002",
               "S-003", "N-003", "N-004", "N-005"] }
}).count()
db.dql_poc_orders.find({ _id: "Q-001" }).count()
~~~

预期：

- 正常记录以及恢复成功的 X-001、P-001 符合目标 schema；
- Q-001 因 Delete 恢复成功而不存在；
- 尚未修复的事件才允许暂时不在目标；
- 源/目标差异必须逐条解释，不能只看总数。

### 16.2 DQL 事件对账

查询：

~~~text
GET /api/dql-events?skip=0&limit=100&order=-failedAt&taskId=TASK_ID
GET /api/dql-events/summary
~~~

逐条核对 eventId、任务 ID、源/目标表、DML 类型、错误类型、状态、恢复尝试次数和最后失败原因。预期每条记录级异常只有一个主事件；重复刷新和打开详情不会增加事件数；每次恢复都有可追踪结果。

### 16.3 清理 POC 资源

确认报告和日志已保存后，只清理本文创建的资源：

~~~bash
docker rm -f dql-poc-source dql-poc-target
~~~

如需停止 TapData 本地服务：

~~~bash
cd /Users/gavinxiao/kit/tapdata/tapdata
docker compose -f docker-compose.local.yml stop mongo tm engine web
~~~

不得删除控制面数据库或其他测试数据。

## 17. 测试记录模板

每个用例执行后按以下格式填写，不要只填写“通过”：

~~~text
用例编号：TC-__
执行时间：
执行人：
任务 ID：
源连接 ID：
目标连接 ID：

实际执行命令/页面操作：

实际请求：
  Method/URL：
  Request Body：
  Response Code：

实际观察：
  任务状态：
  源端结果：
  目标端结果：
  DQL 事件 ID：
  errorType：
  status：
  recoveryAttempts：

预期与实际差异：
最终结论：PASS / FAIL / BLOCKED
失败证据：日志、截图、Network HAR 或响应体路径
~~~

最终 POC 结论必须同时给出：

1. 正常 CDC 是否联通；
2. 共享故障是否走任务级重试/错误；
3. 记录级异常是否进入 DQL 且不拖停任务；
4. Web 的列表、筛选、刷新、详情、预览、恢复是否逐项调用真实 API；
5. 恢复成功后目标数据和事件状态是否逐条对账；
6. 哪些错误类型是当前 MongoDB POC 实际验证出的，哪些严格枚举因连接器错误码未提供而未验证。
