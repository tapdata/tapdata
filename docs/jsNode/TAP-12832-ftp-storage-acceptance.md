# TAP-12832 文件连接 JS 操作验收手册

## 一、验收前置条件

1. 创建 FTP 连接：
   - 源连接名称：`Source-ftp`
   - 目标连接名称：`target-ftp`
2. 创建增强 JS 任务，DAG 示例：

   ```text
   源数据库 -> enhanced JS -> 下游数据库
   ```

3. FTP 节点不需要放入 DAG。
4. JS 节点中使用对象：`storage`。

## 二、场景 1：FTP 文件转发完成后继续数据库同步

### 用户故事

源数据事件到达增强 JS 节点后，先将 `Source-ftp` 的文件复制到 `target-ftp`。文件复制成功后，事件继续流向下游数据库；文件复制失败时，事件不能写入下游数据库。

### 测试数据

源 FTP 文件：`in/a.txt`
目标 FTP 文件：`out/a.txt`

输入事件：

```json
{
  "id": "1001",
  "sourceFtpPath": "in/a.txt",
  "targetFtpPath": "out/a.txt",
  "name": "demo"
}
```

### JS 代码

```javascript
function process(record) {
  var result = storage.update("target-ftp", {
    action: "copy",
    source: {
      connection: "Source-ftp",
      path: record.sourceFtpPath
    },
    target: {
      path: record.targetFtpPath
    }
  }, {
    overwrite: "overwrite"
  });

  if (!result || result.status !== "copied") {
    throw new Error("FTP file transfer was not completed");
  }

  record.ftpTransferStatus = result.status;
  record.ftpTargetPath = record.targetFtpPath;
  return record;
}
```

### 预期结果

- `target-ftp/out/a.txt` 创建成功，文件内容与源文件一致。
- 下游数据库收到该事件。
- 下游数据库中的事件包含 `ftpTransferStatus=copied`。
- 源文件不存在、FTP 无权限或目标连接失败时，JS 节点报错，事件不继续写入下游数据库。

## 三、场景 2：按事件条件执行 FTP 文件转发

### 用户故事

只有满足业务条件的事件才执行 FTP 文件转发，其他事件直接继续正常数据同步。

### JS 代码

```javascript
function process(record) {
  if (record.fileReady !== true) {
    return record;
  }

  var result = storage.update("target-ftp", {
    action: "copy",
    source: {
      connection: "Source-ftp",
      path: record.sourceFtpPath
    },
    target: {
      path: record.targetFtpPath
    }
  }, {
    overwrite: "skip"
  });

  if (!result ||
      (result.status !== "copied" && result.status !== "reused")) {
    throw new Error("FTP file transfer was not completed");
  }

  record.ftpTransferStatus = result.status;
  return record;
}
```

### 预期结果

- `fileReady=false`：不执行 FTP 操作，事件继续进入下游数据库。
- `fileReady=true`：执行文件转发，首次返回 `copied`。
- 目标文件已存在时返回 `reused`，事件继续进入下游数据库。
- 文件转发失败时，当前事件不进入下游数据库。

## 四、场景 3：向目标 FTP 写入事件生成的文件

### 用户故事

增强 JS 根据当前事件内容生成 JSON 文件并写入 `target-ftp`。

### JS 代码

```javascript
function process(record) {
  var result = storage.update("target-ftp", {
    action: "write",
    target: {
      path: "json_" + record.id + ".json"
    },
    content: JSON.stringify(record)
  }, {
    overwrite: "overwrite"
  });

  if (!result || result.status !== "written") {
    throw new Error("FTP file write was not completed");
  }

  return record;
}
```

### 预期结果

- `target-ftp/out/{record.id}.json` 创建成功。
- 文件内容是当前事件的 JSON 内容。
- 写入失败时，当前事件报错，不继续下游处理。

## 五、场景 4：查询、判断存在和删除 FTP 文件

### 用户故事

增强 JS 根据业务条件查询 FTP 文件，必要时删除源文件。

### JS 代码

```javascript
function process(record) {
  var exists = storage.exists("Source-ftp", record.sourceFtpPath);

  if (exists) {
    var file = storage.find("Source-ftp", {
      path: record.sourceFtpPath
    }, null);
    record.sourceFileSize = file ? file.size : null;
  }

  if (exists && record.deleteSource === true) {
    storage.delete("Source-ftp", {
      path: record.sourceFtpPath
    }, null);
  }

  return record;
}
```

### 预期结果

- 文件存在时，`sourceFileSize` 返回文件大小。
- `deleteSource=true` 时，源 FTP 文件删除成功。
- 文件不存在时，不删除文件，事件仍可继续下游处理。

## 六、场景 5：FTP 连接异常

### 用户故事

FTP 文件操作过程中发生连接中断或权限错误时，不能将未完成的文件转发结果继续写入下游数据库。

### 验证步骤

1. 执行场景 1 的 JS 代码。
2. 执行过程中断开 FTP 连接，或配置无效的 FTP 权限。
3. 检查 JS 节点和下游数据库结果。

### 预期结果

- JS 节点返回错误。
- 当前事件不写入下游数据库。
- 恢复 FTP 连接后，后续事件可以重新执行文件操作。
