# TAP-12832 FTP storage 功能验收手册

> 本手册只覆盖当前实现已经提供的能力。未配置真实 FTP 服务时，可以先执行代码级验收；不能用 fake executor 结果替代真实 FTP 命令、网络重试和多 worker 验收。

## 1. 验收前置条件

1. 编译并安装与当前分支匹配的 `tapdata-common-lib` shared file API 和 PDK API。
2. 部署包含以下变更的 engine/connector：
   - `StorageExecutorsManager`、`PdkStorageExecutor`、`StorageFacade`。
   - enhanced JS 节点的 `storage` 注入。
   - FTP FileStorageFunction 注册及 FTP 资源生命周期修复。
3. 在连接管理创建两个 FTP 连接：`source-ftp`、`target-ftp`。连接 rootPath 建议分别配置为测试目录。
4. 创建一个 enhanced JS 任务：DAG 中不放 FTP source/target 节点，只保留业务输入、JS 节点和下游数据库节点。
5. 准备源 FTP 文件 `/in/a.txt`，内容为 `tapdata-12832`，目标 FTP `/out` 目录初始为空。

## 2. 代码级自动化验收

### 2.1 common file operation/session

```bash
mvn -Dmaven.repo.local=/private/tmp/tapdata-m2/repository -o \
  -pl iengine/iengine-common \
  -Dtest=DefaultFileStorageSessionManagerTest,DefaultFileOperationServiceTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

预期：4 个测试通过，覆盖 session 复用、draining invalidate、idle eviction、临时文件删除、最终路径发布和 source stream close。

### 2.2 JS facade/executor

在 engine 基线问题修复或使用现有定向 surefire 方式后运行：

```bash
mvn -Dmaven.repo.local=/private/tmp/tapdata-m2/repository -o \
  -f iengine/iengine-app/pom.xml \
  org.apache.maven.plugins:maven-surefire-plugin:3.0.0:test \
  -Dtest=StorageExecutorsManagerTest,StorageFacadeTest \
  -DsurefireArgLine=
```

预期：8 个测试通过，覆盖 single-flight、失败退避后恢复、invalidate 重建、100 次重复访问只创建一个 executor、write/copy、find/exists/delete 和不支持 action 错误。

## 3. 业务功能场景

### 场景 A：DAG 无 FTP 节点，按事件直接写入

脚本：

```javascript
function process(record) {
  return storage.update('target-ftp', {
    action: 'write',
    target: { path: '/out/' + record.id + '.json' },
    content: JSON.stringify(record.payload)
  }, { overwrite: 'overwrite' });
}
```

步骤：

1. 输入一条业务 record。
2. 确认 JS 节点成功返回，目标 FTP 出现对应文件。
3. 下载目标文件，确认内容与 `record.payload` 一致。
4. 确认任务 DAG 中不存在 FTP 节点。

预期：storage 通过连接名称取得 FTP 能力；脚本不需要连接配置、账号或密码。

### 场景 B：按事件把 source FTP 文件复制到 target FTP

脚本：

```javascript
function process(record) {
  var result = storage.update('target-ftp', {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: record.sourcePath
    },
    target: { path: record.targetPath }
  }, {
    overwrite: 'skip',
    verify: 'size',
    retryTimes: 2
  });
  record.storageResult = result;
  return record;
}
```

输入：`sourcePath=/in/a.txt`、`targetPath=/out/a.txt`。

预期：

- target FTP 文件内容、文件大小与 source FTP 一致。
- 返回 `status=copied`，并包含 `sourcePath`、`targetPath`、`bytes`、`attempts`。
- 文件内容不会进入 JS 之外的重复全量副本；复制由 Java 文件服务完成。

### 场景 C：覆盖策略

分别使用 `overwrite=skip`、`overwrite=overwrite`、`overwrite=fail` 执行目标已存在的复制。

预期：

- `skip` 返回 `reused`，不覆盖目标。
- `overwrite` 返回 `copied`，目标内容更新。
- `fail` 抛出 `FILE_TARGET_CONFLICT`。

### 场景 D：路径安全和不存在文件

使用 `/in/../secret.txt`、空路径、控制字符路径和不存在 source 文件。

预期：

- 非法路径返回 `FILE_PATH_FORBIDDEN`。
- 不存在 source 返回 `FILE_NOT_FOUND`。
- 不产生目标半文件，不创建 `file_operation_ledger`。

### 场景 E：连续事件连接复用

1. 发送至少 100 条会调用同一 `target-ftp` 的 record。
2. 统计 engine 日志、PDK 初始化日志和 FTP server 控制连接数。
3. 任务结束后停止节点，继续观察 FTP session 和 PDK associate 是否释放。

预期：

- 同一个 JS processor node 实例内只发生一次 executor 创建；后续事件命中缓存。
- session 数量受 manager 上限控制，不随事件数线性增长。
- 节点关闭后 executor、session、底层 storage 和 PDK associate id 释放。

### 场景 F：FTP 断连恢复

在 copy 的 source retrieve 或 target store 阶段断开控制/数据连接。

预期：

- 可恢复的远端错误进入有限重试。
- 坏 session 被 invalidate，不被下一次事件继续复用。
- 重试耗尽时当前 JS 调用抛出异常，用户脚本可以捕获或让当前事件失败。

## 4. 兼容性场景

### 场景 G：Mongo aggregate 不受影响

使用现有脚本：

```javascript
function process(record) {
  var mongo = ScriptExecutorsManager.getScriptExecutor('mongo-test');
  var rows = mongo.aggregate({
    database: 'test',
    collection: 'user',
    pipeline: []
  });
  record.aggregateRows = rows.length;
  return record;
}
```

预期：仍通过 `ScriptExecutor`/`ExecuteCommandFunction` 执行；不要求改成 `storage`。

### 场景 H：standard JS 不暴露 storage

在 standard JS 中执行 `typeof storage`。

预期：不会获得文件 storage facade；standard JS 原有记录转换行为保持不变。

### 场景 I：非 FTP 协议边界

使用 SFTP 或其他 file connection 名称调用 storage。

预期：当前 engine 明确返回 `FILE_UNSUPPORTED_OPERATION`，不得静默按 FTP 建连。FTPS 的 `ftpSsl=true` 也不得静默降级为明文 FTP。

## 5. 资源回归场景

在 connectors 仓库执行已提交的 FileConnector/FTP/SFTP 测试，并重点观察：

- connect/login/config 初始化失败后 FTP client 是否 disconnect。
- raw input/output stream 重复 close 是否只完成一次 pending command。
- FileConnector stop 时 merge/release 异常是否仍执行 storage destroy 和 executor shutdown。
- CSV/JSON/Excel discovery 正常、空文件、提前返回、解析异常后 storage 是否 destroy。
- SFTP host key、timeout、channel close 和 unsupported move 行为是否符合配置。

## 6. 当前验收限制

以下结果不能仅通过本地 fake 单元测试宣称完成，需在部署环境补测并记录：

- 真实 FTP server 的 Unicode 路径、断连、权限错误、rename 和大文件传输。
- 多 worker 并发访问同一 FTP 的真实控制连接数量和 cwd 隔离。
- 真实任务停止/取消后的线程、FTP session、临时文件数量。
- 监控平台中的 operation/session/PDK 指标和灰度开关；本期代码未新增 feature flag 和指标后端。
