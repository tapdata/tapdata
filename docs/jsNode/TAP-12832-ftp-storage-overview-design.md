# TAP-12832：增强 JS 节点通过 `storage` 操作 FTP 方案概要设计

> 文档类型：方案概要设计  
> Jira： [TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832)  
> 需求标题：`【MGM】增强 JS 节点支持 FTP 文件同步后继续下游数据库同步`  
> 编写日期：2026-09-08  
> 适用范围：TapData 数据转换任务、增强 JS 节点、FTP/FTPS 文件连接器

## 1. 结论摘要

建议采用“标准连接管理 + 增强 JS `storage` 门面 + 引擎侧文件存储适配器”的实现方式：

1. 用户先在 TapData 中创建源 FTP 和目标 FTP 连接，脚本只引用连接名称，不直接携带主机、账号或密码。
2. 增强 JS 节点提供面向文件的 `storage` API，首期支持查询、复制/覆盖更新、删除和存在性检查；`storage.update` 是 FTP 文件转发的主入口。
3. 文件流转、临时文件、校验、重试和幂等记录均在引擎/连接器侧完成，不把大文件读入 JS 堆内存。
4. 增强 JS 节点支持可选的任务级 `beforeTask(context)` 生命周期。FTP 转发在该阶段执行一次并阻塞后续数据处理；成功后才允许 `process(record)` 向下游输出记录。
5. 任一文件传输、校验、连接或超时错误都以结构化异常结束 JS 节点，阻止下游数据库节点继续写入。
6. 任务重试通过持久化的 operation ledger 和用户提供的确定性 `idempotencyKey` 恢复，已成功文件只做目标侧复核，不重复传输。

该方案既满足 TAP-12832 的 FTP→FTP 转发场景，又保留了后续扩展到 SFTP、S3、SMB 等文件存储的边界，不把 FTP 细节泄漏到业务脚本中。

## 2. 需求分析

### 2.1 业务目标

MGM 需要在一个 TapData 同步任务中先完成文件转发，再处理正常业务数据：

```text
源 FTP 文件准备完成
        │
        ▼
增强 JS：读取/筛选源文件 → 写入目标 FTP → 校验落盘
        │
        ├── 失败：任务失败/重试，下游数据库不写入
        │
        └── 成功：继续输出数据记录 → 下游数据库同步
```

### 2.2 Jira 已确认的要求

| 类别 | 要求 |
| --- | --- |
| 连接 | 支持配置源 FTP、目标 FTP、目录和文件筛选规则 |
| 传输 | 支持单个或多个文件，保留文件名及目录结构 |
| 正确性 | 支持目标文件存在性、大小或校验值验证 |
| 编排 | FTP 文件同步成功后才允许下游数据库同步 |
| 失败 | 连接失败、文件不存在、超时、校验失败时阻断下游 |
| 幂等 | 任务重试或重启后不重复传输已成功文件，不重复写入下游数据 |
| 可观测性 | 记录文件清单、结果、开始/结束时间、失败阶段和错误原因 |

### 2.3 仓库现状与约束

当前代码体现出以下事实：

* `tapdata-web/packages/business/src/components/ConnectorForm.vue` 使用通用 PDK 表单、`pdkHash` 和连接 ID 创建/测试连接；新增 FTP 连接应复用该机制，不应在 JS 节点中重复设计凭据表单。
* `tapdata-web/packages/dag/src/nodes/JsProcessor.js` 将增强 JS 与标准 JS 区分，增强 JS 当前允许使用完整内置函数；`JsProcessor` 将增强 JS 文档指向 `/appendix/enhanced-js`。
* `tapflow/tapflow/lib/data_pipeline/nodes/js.py` 保存 `script` 和 `declareScript`，并在未包含 `function process(record)` 时自动补齐函数头。因此新增 `beforeTask(context)` 约定可以保持现有脚本格式兼容；`declareScript` 继续用于模型声明，不用于承载 FTP 操作。
* `tapdata-web/packages/component/src/JsEditor.vue` 通过 `/api/Javascript_functions` 拉取系统/自定义函数并提供代码补全。新增 `storage` 方法时应同时登记函数签名和参数说明。
* 已发布的增强 JS 文档中，`aggregate` 的用法是 `ScriptExecutorsManager.getScriptExecutor('connection-name').aggregate({ database, collection, pipeline })`，返回数组结果。该模式说明“连接名解析 + 远端执行器”的能力已有产品认知，可作为 `storage` 门面的交互参考。
* 当前 `tapdata-connectors-enterprise` 源码中未发现 FTP 连接器实现；本设计将 FTP PDK/连接器视为依赖。如果目标分支没有可用 FTP PDK，需要先在连接器仓库补齐连接器能力，不能由 JS 脚本直接打开原始 socket 作为替代。

## 3. 范围与非目标

### 3.1 本期范围

* 用户通过标准连接管理创建 FTP 连接。
* 增强 JS 节点解析连接名并执行文件操作。
* FTP 文件列表、复制/覆盖更新、删除、存在性检查。
* 跨 FTP 连接的流式复制。
* 文件筛选、目录递归、目录结构保留、覆盖策略和传输后校验。
* 任务级幂等键、分布式互斥、重试恢复和结构化日志。
* 传输成功/失败对 JS 节点输出的门控。
* 增强 JS API 文档、代码补全和端到端验收测试。

### 3.2 非目标

* 本期不重建通用 Workflow 产品；只在现有数据转换任务中保证 JS 节点到下游的数据依赖。
* 不在脚本中暴露 FTP 密码、私钥或完整连接配置。
* 不将 TB 级大文件切片、断点续传和跨地域带宽优化作为首期承诺。
* 不把 FTP 文件解析成表格数据；本需求的文件内容按二进制流转发。
* 不默认删除源文件；删除/移动必须是显式 API 调用和显式配置。
* 不在标准 JS 节点开放 `storage` 外部调用能力。

## 4. 方案比较

### 4.1 方案 A：直接扩展现有 `ScriptExecutor`

示例：

```javascript
var ftp = ScriptExecutorsManager.getScriptExecutor('target-ftp')
ftp.update({ source: 'source-ftp', path: '/in/a.csv', targetPath: '/out/a.csv' })
```

优点是改动小，可以沿用 `aggregate` 的连接名解析方式。缺点是把数据库执行器语义和文件传输语义混在一起，无法清晰表达流式传输、校验、幂等、临时文件等文件特有能力；后续支持 S3/SMB 时接口会继续膨胀。

### 4.2 方案 B：新增 `storage` 门面和适配器层（推荐）

示例：

```javascript
storage.update(
  'target-ftp',
  {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: '/inbound',
      pattern: '*.csv',
      recursive: true,
    },
    target: {
      path: '/outbound',
      preservePath: true,
    },
  },
  {
    overwrite: 'skip',
    verify: 'size',
    idempotencyKey: `mgm-${businessDate}`,
  },
)
```

`storage` 负责稳定的脚本契约，`FtpStorageAdapter` 负责 FTP 协议细节，后续可以新增 `SftpStorageAdapter`、`S3StorageAdapter` 而不改变脚本入口。该方案分层清晰，也能将凭据、流式传输和幂等状态留在受控运行时中，推荐采用。

### 4.3 方案 C：新增专用“FTP 文件传输节点”

把 FTP 转发做成独立节点，由节点属性配置源连接、目标连接、路径、筛选和校验。

该方案的运行语义最清晰，且更容易做配置校验；但需要新增节点类型、表单、任务序列化、运行器和监控展示，交付面明显大于本需求描述的“增强 JS 中使用内置函数”。可作为后续产品化方向，不作为 TAP-12832 首期方案。

## 5. 推荐架构

### 5.1 分层结构

```text
┌──────────────────────────────────────────────────────────────┐
│ TapData Web                                                   │
│ 连接创建/测试 │ 增强 JS 编辑器 │ storage 文档/代码补全          │
└──────────────────────────────┬───────────────────────────────┘
                               │ 保存 task script / connection ref
┌──────────────────────────────▼───────────────────────────────┐
│ Task / Engine                                                 │
│ Enhanced JS Runtime                                           │
│  ├─ beforeTask(context)                                        │
│  ├─ process(record)                                            │
│  └─ storage facade                                             │
│      ├─ ConnectionResolver(name → connection id/config)        │
│      ├─ OperationLedger(idempotency/lock/result)               │
│      └─ StorageAdapter                                           │
│          └─ FtpStorageAdapter                                   │
└──────────────────────────────┬───────────────────────────────┘
                               │ PDK command / stream API
┌──────────────────────────────▼───────────────────────────────┐
│ Source FTP  ─────── streaming transfer ───────► Target FTP      │
└───────────────────────────────────────────────────────────────┘
```

### 5.2 关键职责

| 组件 | 职责 | 不负责 |
| --- | --- | --- |
| Connection UI/Service | 创建、保存、权限校验、脱敏返回 FTP 连接 | 执行文件复制 |
| `ConnectionResolver` | 按租户和任务权限将连接名解析为内部连接 ID；缓存非敏感元数据 | 将密码传给脚本 |
| `storage` facade | 校验参数、统一返回值、调用幂等/锁、选择适配器 | 实现 FTP 协议 |
| `FtpStorageAdapter` | 列目录、打开流、写临时文件、重命名、获取元数据、校验 | 解释 JS 脚本业务逻辑 |
| `OperationLedger` | 记录操作和文件状态、互斥、恢复 | 保存业务数据 |
| Enhanced JS Runtime | 调用 `beforeTask`、执行 `process`、传播异常和输出 | 直接管理连接凭据 |
| 下游数据库节点 | 仅消费已经通过 JS 门控的记录 | 判断 FTP 是否成功 |

## 6. 连接设计

### 6.1 用户配置流程

1. 用户进入连接管理，选择 FTP PDK。
2. 配置连接名称、协议模式、主机、端口、根目录和认证信息。
3. 通过通用 PDK `Test` 命令验证连通性和目录权限。
4. 保存连接后，在增强 JS 脚本中按连接名称引用。
5. 脚本运行时由引擎按当前租户、用户权限和任务权限解析连接。

### 6.2 首期连接属性

具体字段以 FTP PDK 的现有 schema 为准，至少应覆盖：

| 属性 | 说明 | 安全要求 |
| --- | --- | --- |
| `name` | TapData 连接名称，任务内作为引用名 | 同租户唯一；脚本只传名称 |
| `host` / `port` | FTP/FTPS 服务地址和端口 | 错误信息不得回显密码 |
| `protocol` | `ftp` 或已支持的 `ftps` | SFTP 是否纳入由连接器能力决定 |
| `username` | 登录用户 | 不写入脚本日志 |
| `password` / `privateKey` | 认证凭据 | 由连接服务加密保存 |
| `rootPath` | 连接默认根目录 | 运行时禁止路径逃逸 |
| `passiveMode` | 被动/主动模式 | 默认使用连接器推荐模式 |
| `connectTimeout` / `readTimeout` | 连接和读写超时 | 可被任务级上限约束 |
| `tls` / 证书配置 | FTPS 安全配置 | 证书内容脱敏展示 |

连接名解析必须使用精确匹配，找不到连接、存在重名或连接类型不是文件存储时直接返回可识别错误。内部传输使用连接 ID 和受控凭据，不允许脚本通过 `rest` 或任意 Java 类自行绕过连接权限。

## 7. `storage` API 概要

### 7.1 设计原则

* 第一个参数是用户可读的连接名称，和现有 `aggregate` 的连接名使用方式一致。
* `data` 描述文件对象或传输动作；大文件内容不经过 JS 参数和返回值。
* 所有外部操作同步返回结果或抛出异常，确保 JS 节点输出天然形成门控。
* 所有写操作支持 `idempotencyKey`；未提供时仅保证当前运行实例内的重复调用可合并，不承诺跨运行幂等。
* 所有路径以连接根目录为边界，拒绝 `..` 逃逸、空目标路径和非法文件名。

### 7.2 方法列表

#### `storage.find(connectionName, query, options)`

查询文件或目录元数据，不读取文件内容。

```javascript
var files = storage.find('source-ftp', {
  path: '/inbound',
  pattern: '*.csv',
  recursive: true,
}, {
  limit: 1000,
  sort: 'modifiedAt',
})
```

返回数组，每项至少包含：

```javascript
{
  path: '/inbound/2026/09/a.csv',
  name: 'a.csv',
  type: 'file',
  size: 1048576,
  modifiedAt: '2026-09-08T02:00:00Z',
  checksum: null
}
```

#### `storage.update(connectionName, data, options)`

首期 FTP 文件转发主入口。推荐以“目标连接”为第一个参数，在 `data.source` 中声明源连接：

```javascript
var result = storage.update(
  'target-ftp',
  {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: '/inbound',
      pattern: '*.csv',
      recursive: true,
    },
    target: {
      path: '/outbound',
      preservePath: true,
    },
  },
  {
    overwrite: 'skip',
    verify: 'size',
    idempotencyKey: 'mgm-${businessDate}',
    maxFiles: 1000,
    maxBytes: 10 * 1024 * 1024 * 1024,
    timeoutMs: 30 * 60 * 1000,
  },
)
```

`action` 首期固定支持 `copy`。后续可扩展 `upload`、`move` 或 `replace`，不改变第一个参数的连接解析方式。

返回结构建议如下：

```javascript
{
  success: true,
  operationId: 'op-xxx',
  idempotent: false,
  files: [
    {
      sourcePath: '/inbound/2026/09/a.csv',
      targetPath: '/outbound/2026/09/a.csv',
      status: 'COPIED',
      size: 1048576,
      checksum: null,
      verified: true,
    },
  ],
  summary: {
    total: 1,
    copied: 1,
    skipped: 0,
    failed: 0,
    bytes: 1048576,
  },
  startedAt: '2026-09-08T02:00:00Z',
  finishedAt: '2026-09-08T02:00:03Z',
}
```

#### `storage.exists(connectionName, path)`

返回目标路径是否存在以及基本元数据，用于脚本自定义校验或补偿。

#### `storage.delete(connectionName, data, options)`

显式删除文件或目录。默认不允许目录递归删除；删除操作要求用户明确传入 `recursive: true`，并记录审计日志。

### 7.3 通用选项

| 选项 | 语义 |
| --- | --- |
| `overwrite` | `skip`：目标相同则跳过；`overwrite`：覆盖；`fail`：目标存在即失败 |
| `verify` | `none`、`size`、`sha256`；首期默认 `size` |
| `preservePath` | 是否保留源目录相对路径 |
| `recursive` / `pattern` | 目录递归和文件筛选 |
| `maxFiles` / `maxBytes` | 单次操作保护上限，受部署配置约束 |
| `connectTimeoutMs` / `readTimeoutMs` | 连接和读写超时 |
| `idempotencyKey` | 业务可复现的幂等标识 |
| `failFast` | 是否首个文件失败即停止；默认 `false`，返回汇总后抛出聚合异常 |
| `dryRun` | 仅列出计划，不写入目标；用于验证筛选规则 |

### 7.4 错误语义

发生以下任一情况时，`storage.update` 不返回“成功”结果，而是抛出结构化 `StorageOperationError`：

* `CONNECTION_NOT_FOUND`：连接名称不存在或当前用户无权限。
* `CONNECTION_TYPE_MISMATCH`：连接不是 FTP/受支持的文件存储类型。
* `AUTH_FAILED`、`CONNECT_TIMEOUT`、`READ_TIMEOUT`：连接或认证失败。
* `PATH_DENIED`、`FILE_NOT_FOUND`：路径不可访问或源文件消失。
* `LIMIT_EXCEEDED`：超过文件数、总大小或执行时长限制。
* `TRANSFER_FAILED`、`PARTIAL_TRANSFER`：写入失败或产生不完整目标文件。
* `VERIFY_FAILED`：目标文件大小或校验值不匹配。
* `IDEMPOTENCY_CONFLICT`：同一个幂等键对应了不一致的操作内容。

异常中必须包含 `operationId`、源/目标连接名、源/目标路径、阶段、重试次数和可脱敏的底层错误；不得包含密码、私钥或完整连接配置。

## 8. 任务执行与依赖门控

### 8.1 为什么不能只把复制逻辑写进 `process(record)`

现有 JS 节点的基本契约是 `function process(record)`。如果用户在其中直接调用 FTP 复制：

* 一个任务可能对每条数据重复执行同一份文件传输。
* 多分片/多线程执行时可能并发复制同一批文件。
* 源端没有数据记录时，`process` 不会被调用，文件前置动作也不会发生。

因此需要增加任务级生命周期约定，不能只增加一个普通函数名。

### 8.2 `beforeTask(context)` 约定

增强 JS 脚本可定义以下可选函数：

```javascript
function beforeTask(context) {
  return storage.update(
    'target-ftp',
    {
      action: 'copy',
      source: {
        connection: 'source-ftp',
        path: '/inbound/' + context.businessDate,
        pattern: '*.csv',
      },
      target: {
        path: '/outbound/' + context.businessDate,
        preservePath: true,
      },
    },
    {
      idempotencyKey: 'mgm-ftp-' + context.businessDate,
      overwrite: 'skip',
      verify: 'size',
    },
  )
}

function process(record) {
  return record
}
```

执行语义：

1. 任务实例启动后，运行时检测脚本是否定义 `beforeTask`。
2. 通过 `OperationLedger` 以任务 ID、JS 节点 ID 和 `idempotencyKey` 获取分布式锁。
3. 首个执行者执行 FTP 操作；其他并发执行者等待并复用成功结果。
4. `beforeTask` 成功后才开始调用 `process(record)` 并向下游输出记录。
5. `beforeTask` 抛错时，JS 节点失败，不输出任何下游记录；任务按现有任务重试策略处理。
6. 不定义 `beforeTask` 的旧脚本保持原有行为，不引入兼容性破坏。

如果引擎当前按分片启动多个 JS 实例，锁必须放在共享持久化存储或任务元数据服务中，不能只放在单个 Agent 的进程内存中。

### 8.3 记录级调用的兼容方式

对于确实需要“每条记录对应一个文件操作”的场景，仍允许在 `process(record)` 中调用 `storage.update`，但必须显式提供确定性幂等键：

```javascript
function process(record) {
  storage.update('target-ftp', {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: record.sourcePath,
    },
    target: {
      path: record.targetPath,
    },
  }, {
    idempotencyKey: 'file-' + record.fileId,
    overwrite: 'skip',
    verify: 'size',
  })
  return record
}
```

编辑器和文档应明确提示：跨任务前置文件同步优先使用 `beforeTask`；直接在 `process` 中复制文件只适合“记录驱动”的文件操作。

## 9. 文件传输流程

```text
beforeTask
   │
   ├─ 解析源/目标连接名
   ├─ 校验目录、筛选规则和操作上限
   ├─ 获取幂等锁和 operation ledger
   ├─ 列出源文件并生成传输计划
   ├─ 对每个文件：
   │    ├─ 查询 ledger/目标元数据
   │    ├─ 已成功且校验一致 → SKIPPED
   │    ├─ 打开源流
   │    ├─ 写入目标临时文件，例如 `.tapdata.part/<operationId>/<name>`
   │    ├─ flush/close
   │    ├─ 大小或 SHA-256 校验
   │    ├─ 重命名为最终目标路径
   │    └─ 写入文件级成功状态
   ├─ 汇总所有文件结果
   ├─ 所有文件成功/跳过 → ledger 标记 SUCCEEDED
   └─ 任一文件失败 → ledger 标记 FAILED，抛出 StorageOperationError
        │
        ▼
process(record) → 下游数据库节点
```

### 9.1 临时文件与一致性

目标端先写临时文件，传输和校验通过后再重命名。这样下游消费者不会读到半文件。若目标 FTP 服务不支持可靠重命名，应将该能力标记为连接器限制，并在严格校验模式下直接失败，而不是伪装为已完成。

### 9.2 文件筛选与目录结构

* 源目录以连接根目录为边界。
* `pattern` 只作用于文件名或相对路径，不支持执行任意脚本表达式。
* `preservePath: true` 时，目标路径由源目录下的相对路径拼接生成。
* 同一传输计划中若两个源文件映射到相同目标路径，操作先失败并报告冲突，不按列表顺序覆盖。

## 10. 幂等、重试与恢复

### 10.1 Operation ledger

建议增加任务运行侧的文件操作记录，最小字段如下：

| 字段 | 说明 |
| --- | --- |
| `tenantId` / `taskId` / `nodeId` | 作用域 |
| `operationId` | 一次逻辑操作 ID |
| `idempotencyKey` | 用户提供的业务幂等键 |
| `requestHash` | 规范化后的操作参数摘要，用于检测幂等键冲突 |
| `status` | `RUNNING`、`SUCCEEDED`、`FAILED`、`EXPIRED` |
| `files` | 每个文件的源路径、目标路径、大小、校验、状态和错误 |
| `leaseOwner` / `leaseExpireAt` | 分布式锁租约 |
| `startedAt` / `finishedAt` | 操作耗时和审计 |

### 10.2 重试规则

* 相同 `idempotencyKey` 和相同 `requestHash` 已为 `SUCCEEDED`：复用结果，重新检查目标文件元数据，不重复传输。
* 状态为 `RUNNING` 且租约未过期：等待持有者完成，不并发复制。
* 状态为 `RUNNING` 但租约已过期：新执行者接管，逐文件恢复。
* 状态为 `FAILED`：允许按任务重试策略重试；成功文件跳过，失败文件重新传输。
* 同一 `idempotencyKey` 的请求参数发生变化：拒绝执行，防止“同一业务批次”被错误复用。
* 未提供 `idempotencyKey`：系统生成运行级 key，只保证一个任务实例内去重；文档和 UI 应提示跨重启幂等需要业务 key。

### 10.3 下游数据库幂等

FTP 操作的幂等不能替代下游数据库的幂等。下游目标仍需沿用 TapData 现有的主键、UPSERT、去重或任务重试语义；本需求的职责是确保 FTP 前置失败时不产生下游输入，并避免成功文件在同一业务批次中重复发送。

## 11. 前端与函数注册改造点

### 11.1 连接管理

* 复用通用连接创建/编辑/测试页面。
* 由 FTP PDK 提供连接表单、能力声明和 `Test` 命令。
* 连接列表显示 FTP/FTPS 类型、连通状态和最近测试结果。
* JS 节点不嵌入连接凭据选择器；脚本按名称引用连接，避免任务配置复制敏感信息。

### 11.2 增强 JS 编辑器

* 在增强 JS 文档中加入 `storage.find/update/exists/delete`、`beforeTask`、幂等和异常说明。
* 在 `/api/Javascript_functions` 系统函数目录中登记 `storage` 相关签名、参数描述和返回值，使 `JsEditor` 自动补全可用。
* 标准 JS 文档和标准 JS 编辑器不展示/不开放 `storage` 外部操作。
* 在增强 JS 节点中增加提示：`storage.update` 是同步调用，文件未成功落盘时会阻断后续数据。
* 调试运行需要明确区分“试运行有外部副作用”和“dryRun”。默认建议以 `dryRun` 或受控测试连接验证筛选规则。

### 11.3 脚本兼容

* 保持现有 `function process(record)` 的编辑方式。
* 允许脚本同时定义 `beforeTask(context)` 和 `process(record)`；不改变 `declareScript` 的模型声明含义。
* 历史脚本未定义 `beforeTask` 时按旧流程运行。

## 12. 安全设计

* 脚本只能按连接名称引用连接，不能读取连接密码、私钥或原始 config。
* `ConnectionResolver` 必须在当前租户和任务权限范围内解析连接，防止通过名称越权访问其他租户连接。
* 路径以连接 `rootPath` 为沙箱边界，拒绝绝对路径越权、`..` 路径逃逸和控制字符。
* `storage` 只在增强 JS 中可用；标准 JS 保持纯记录转换定位。
* 连接、文件传输和脚本日志不打印密码、私钥、token；错误堆栈需脱敏。
* FTP 明文传输应在连接配置和文档中明确风险；生产环境优先使用 FTPS 或由网络隔离提供保护。
* `maxFiles`、`maxBytes`、`timeoutMs` 受系统级上限约束，脚本不能通过传入更大值绕过部署保护。
* 删除操作必须显式调用，并保留操作人、任务、连接和路径审计信息。

## 13. 可观测性

每次操作至少输出以下结构化字段：

```json
{
  "event": "storage_operation",
  "operationId": "op-xxx",
  "taskId": "task-xxx",
  "nodeId": "node-xxx",
  "operation": "copy",
  "sourceConnection": "source-ftp",
  "targetConnection": "target-ftp",
  "sourcePath": "/inbound/a.csv",
  "targetPath": "/outbound/a.csv",
  "phase": "VERIFY",
  "status": "SUCCESS",
  "bytes": 1048576,
  "durationMs": 3000,
  "attempt": 1
}
```

日志要求：

* 文件级和汇总级都可查询；失败时能定位到连接、路径和阶段。
* 只记录文件元数据，不记录文件内容。
* 明确区分 `COPIED`、`SKIPPED_IDEMPOTENT`、`SKIPPED_EXISTS`、`FAILED`、`VERIFIED`。
* 节点失败信息中关联 `operationId`，便于从任务日志追到文件清单。
* 运行记录中只保留脱敏后的错误摘要，完整异常按现有任务日志权限查看。

## 14. 测试与验收设计

### 14.1 单元测试

* 连接名不存在、重名、无权限和类型不匹配。
* 路径规范化、根目录边界、`..` 逃逸和目标路径冲突。
* `pattern`、递归、目录结构保留和文件数/字节数限制。
* `overwrite=skip/overwrite/fail` 三种策略。
* 大小校验、SHA-256 校验和校验失败。
* operation ledger 状态转换、请求 hash 冲突、租约过期接管。
* `beforeTask` 成功/失败时 JS 输出门控。
* 旧脚本无 `beforeTask` 时保持兼容。

### 14.2 FTP 适配器集成测试

使用可控的本地 FTP/FTPS 测试服务覆盖：

* 单文件、多文件、递归目录和特殊字符文件名。
* 目标目录不存在时创建或按配置失败。
* 传输中断、源文件消失、目标权限不足、连接超时和重连。
* 临时文件残留清理和最终文件不可见窗口。
* 大文件流式传输，验证 Agent/Engine 内存不随文件大小线性增长。

### 14.3 任务级端到端测试

| 场景 | 预期结果 |
| --- | --- |
| 源 FTP 有 1 个匹配文件，目标为空 | 文件复制并校验成功，下游 DB 写入 |
| 源 FTP 有多个匹配文件 | 所有文件完成后才输出数据，文件数和内容一致 |
| 源连接失败 | JS 节点失败，下游 DB 无写入 |
| 目标写入失败 | JS 节点失败，目标不出现半文件，下游 DB 无写入 |
| 校验不一致 | JS 节点失败，记录 `VERIFY_FAILED`，下游 DB 无写入 |
| 任务在一半文件后重试 | 已成功文件跳过，失败文件恢复，最终不重复传输 |
| 多实例并发执行同一幂等键 | 只有一个实际传输者，其余复用结果 |
| 同一批次重复运行 | 使用同一业务 key 时目标文件不重复写入 |
| 源目录为空 | `beforeTask` 按策略返回空成功或失败；行为必须由任务配置明确，不能静默跳过 |
| JS 调试 `dryRun` | 只产生传输计划，不写入目标 |

### 14.4 对应 Jira 验收标准

* AC-01：源/目标 FTP 连接和规则配置完成后，可通过增强 JS 传输匹配文件，并保留文件名和目录结构。
* AC-02：连接超时、文件缺失或校验失败时，任务进入失败/重试状态，下游数据库不写入。
* AC-03：任务重启或重试后使用同一幂等键恢复，已成功文件不重复传输，下游数据库不产生非预期重复写入。

## 15. 发布与兼容策略

1. 先确认目标发布分支具备 FTP/FTPS PDK；若没有，先交付 FTP 连接器和连接测试能力。
2. 以 feature flag 控制增强 JS `storage` 和 `beforeTask`，默认只对测试环境开放。
3. 首期只开放 `find`、`update(copy)`、`exists`、`delete`，限制文件数、总大小和执行时长。
4. 在测试环境验证 FTP→FTP、失败阻断、重试恢复、并发锁和审计日志。
5. 发布时同步更新增强 JS 文档、函数补全元数据和用户操作说明。
6. 旧标准 JS、旧增强 JS 脚本以及不使用 `beforeTask` 的任务行为保持不变。

## 16. 待确认事项

以下事项不阻塞本方案的分层设计，但在开发前需要产品、研发和客户确认：

| 问题 | 建议默认值/处理 |
| --- | --- |
| FTP 是否包含 FTPS，是否纳入 SFTP | 首期支持现有 FTP PDK 能力；FTPS 跟随连接器，SFTP 单独评估 |
| 单次最大文件数和总大小 | 由部署配置和 Agent 能力限定，MGM POC 需给出实际规模 |
| 默认完整性校验 | 默认文件大小；有能力时允许 SHA-256 |
| 目标文件已存在时行为 | 默认 `skip`，若大小/校验不一致则重新传输或失败，按策略明确 |
| 源目录为空是否算成功 | 默认按业务配置；严格依赖场景建议配置为失败 |
| 是否传输成功后删除源文件 | 默认不删除；如需删除，使用显式 `storage.delete` |
| 无输入记录时是否仍执行 FTP 前置动作 | 采用 `beforeTask`，使行为与数据行数解耦 |
| 文件操作记录保留时长 | 复用任务运行记录保留策略；需补充容量和清理配置 |

## 17. 参考资料与代码定位

### Jira

* [TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832)：本需求原始背景、目标、流程和验收标准。

本设计的需求来源仅为 TAP-12832；其他 Jira 不作为本方案的需求依据。

### 仓库

* `tapdata-web/packages/business/src/components/ConnectorForm.vue`：通用 PDK 连接配置、测试和命令调用。
* `tapdata-web/packages/dag/src/nodes/JsProcessor.js`：增强 JS 节点配置及与标准 JS 的区别。
* `tapdata-web/packages/dag/src/components/form/js-processor/index.tsx`：增强/标准 JS 文档入口、脚本编辑和试运行入口。
* `tapdata-web/packages/component/src/JsEditor.vue`：系统函数目录加载、代码补全和函数文档提示。
* `tapflow/tapflow/lib/data_pipeline/nodes/js.py`：JS 脚本序列化和 `process(record)` 兼容包装。
* `data/components/webroot/docs/appendix/enhanced-js/index.html`：现有增强 JS 内置函数文档，包含 `aggregate` 和连接名执行器示例。

## 18. 方案落地判定

该方案满足 TAP-12832 的核心验收前提：

* 用户可以先创建 FTP 连接，再在增强 JS 中按名称操作 FTP。
* 文件传输使用引擎侧流式能力，不要求 JS 承载大文件。
* `beforeTask` 确保文件操作先于数据输出，不依赖第一条业务记录。
* 统一异常传播确保传输失败时下游数据库不获得输入。
* operation ledger、幂等键和目标校验保证重试可恢复、成功文件不重复传输。
* 结构化日志和文件级结果满足问题定位及验收取证需要。

实现前应优先确认 FTP PDK 是否已存在以及增强 JS 运行器是否支持任务级生命周期；如果二者任一不存在，应将其作为明确的基础能力子任务，而不是在业务脚本中绕过平台连接和任务执行模型。
