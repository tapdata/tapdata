# TAP-12832 文件连接 JS 操作验收手册

本文档只覆盖当前实现已经提供的能力。真实 FTP 服务、权限、断连和吞吐需要在部署环境验收，不能用 fake executor 单测替代。

## 1. 验收前置条件

1. 使用包含当前 engine、connectors 变更的构建产物；`tapdata-common-lib` 保持既有公共 API。
2. 创建两个 FTP 连接：`source-ftp`、`target-ftp`，准备源文件 `in/a.txt`。
3. 创建 enhanced JS 任务，DAG 中不放 FTP 节点。
4. 确认 JS 节点可以访问连接名称对应的 `Connections` 配置。

## 2. 自动化验证

engine 定向测试：

```bash
mvn -o -Dmaven.repo.local=/Users/gavinxiao/.m2/repository \
  -pl iengine/iengine-app -am \
  -Dtest=StorageFacadeTest,StorageExecutorsManagerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

预期：`StorageExecutorsManagerTest` 6 项、`StorageFacadeTest` 5 项，共 11 项通过。

文件 storage 和文件 Connector 编译：

```bash
mvn -o -Dmaven.repo.local=/Users/gavinxiao/.m2/repository \
  -pl file-storages/local-file,file-storages/ftp-file,file-storages/smb-file,\
file-storages/sftp-file,file-storages/s3fs-file,file-storages/oss-file \
  -am -DskipTests -Dmaven.test.skip=true compile
```

## 3. 业务场景

### 场景 A：DAG 无 FTP 节点时写入

```javascript
function process(record) {
  return storage.update("target-ftp", {
    action: "write",
    target: { path: "out/" + record.id + ".json" },
    content: JSON.stringify(record.payload)
  }, { overwrite: "overwrite" });
}
```

预期：JS 成功返回，FTP 目标目录出现文件，文件内容与 `record.payload` 一致；DAG 中不存在 FTP 节点。

### 场景 B：FTP 到 FTP 按事件复制

```javascript
function process(record) {
  if (!record.sourcePath) return record;
  var result = storage.update("target-ftp", {
    action: "copy",
    source: { connection: "source-ftp", path: record.sourcePath },
    target: { path: record.targetPath }
  }, { overwrite: "skip" });
  record.storageResult = result;
  return record;
}
```

预期：目标文件内容和长度与源文件一致，返回 `status=copied`；目标已存在时 `skip` 返回 `status=reused`。

### 场景 C：查询、存在性、删除和覆盖策略

```javascript
var file = storage.find("target-ftp", { path: "out/a.json" }, null);
var exists = storage.exists("target-ftp", "out/a.json");
var removed = storage.delete("target-ftp", { path: "out/a.json" }, null);
```

分别验证 `overwrite=skip`、`overwrite=overwrite`、`overwrite=fail`。非法 action、空路径、目标冲突等应返回引擎本地 `StorageOperationException`，不会写入事件 ledger。

### 场景 D：同连接复用和异常恢复

1. 连续发送 1000 条调用同一连接的事件，观察 PDK/storage 初始化次数和 FTP 登录次数。
2. 预期同一 JS processor 实例内同名连接只创建一个 executor，后续事件命中缓存。
3. 中断 FTP 控制连接，预期当前调用报错并使 executor 失效；下一次调用可以重新创建连接。
4. 停止任务或关闭 JS 节点，确认 storage、FTP client、PDK associateId、state map 和 table map 释放。

### 场景 E：大文件和同连接复制

- 不同 FTP 连接复制大文件时，确认 Java 层流式传输，JS 不接触完整文件内容。
- 同一个连接 source/target 相同的复制场景，确认实现先落本地临时文件再写目标，避免同一 FTP client 持有读锁时直接写入。
- 检查复制完成后临时文件被删除，输入/输出流最终关闭。

## 4. 兼容性场景

### Mongo aggregate

```javascript
var mongo = ScriptExecutorsManager.getScriptExecutor("mongo-test");
var rows = mongo.aggregate({ database: "test", collection: "user", pipeline: [] });
```

预期仍通过原有数据库 `ScriptExecutor` 执行。`ScriptExecutorsManager.getScriptExecutor("target-ftp")` 不作为 FTP 文件 API。

### 非 FTP 协议

当前引擎已有多种 storage class 映射，但 FTP 是本需求首要验收协议。对 SFTP、SMB、S3FS、OSS、NFS、Local，应先确认对应 PDK class、参数和真实环境，再逐协议验收；不能以 FTP 验收结果代替其他协议验收。

## 5. 资源回归检查

重点检查：

- FTP/SFTP 初始化失败是否断开半初始化资源；
- raw stream 重复关闭是否不会重复完成协议 pending command；
- FileConnector 停止时任一清理异常是否仍继续清理其他资源；
- Local/SMB 输出流、OSS/S3FS 对象流是否在异常和正常路径关闭；
- `InputStream.available()` 不再被当作 OSS/S3FS 远端文件总长度。
