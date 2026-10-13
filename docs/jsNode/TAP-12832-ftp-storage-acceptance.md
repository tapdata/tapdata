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

## 七、场景 6：通过 HTTP 下载二进制文件并写入目标 FTP

### 用户故事

增强 JS 根据事件中的 HTTP 文件 URL 下载 PNG 文件，并将原始二进制内容写入 `target-ftp`。

### 输入事件

```json
{
  "id": 4,
  "url": "https://tapdata.io/assets-global.website-files.com/61acd54a60fc632b98cbac1a/tab_public-health-authority.png",
  "file_type": "image_png"
}
```

### JS 代码

```javascript
function process(record) {
  var content = httpUtil.downloadBytes(record.url);

  var result = storage.update("target-ftp", {
    action: "write",
    contentType: "bytes",
    target: {
      path: "save_" + record.id + ".png"
    },
    content: content
  }, {
    overwrite: "overwrite"
  });

  if (!result || result.status !== "written") {
    throw new Error("PNG file write was not completed");
  }

  return record;
}
```

### 预期结果

- `target-ftp/save_4.png` 创建成功。
- 文件可以正常打开，文件内容为 HTTP URL 返回的 PNG 二进制内容。
- 文件不是 `[-119, 80, 78, ...]` 形式的文本。
- HTTP 下载失败或 FTP 写入失败时，JS 节点报错，当前事件不继续下游处理。

## 八、场景 7：通过 HTTP 流写入目标 FTP

### 用户故事

增强 JS 以流方式下载 HTTP 文件并写入 `target-ftp`，写入结束后由脚本关闭 HTTP 流。

### JS 代码

```javascript
function process(record) {
  var input = httpUtil.openStream(record.url);
  try {
    var result = storage.update("target-ftp", {
      action: "write",
      contentType: "stream",
      target: {
        path: "save_" + record.id + ".png"
      },
      content: input
    }, {
      overwrite: "overwrite"
    });

    if (!result || result.status !== "written") {
      throw new Error("HTTP stream FTP write was not completed");
    }
  } finally {
    input.close();
  }

  return record;
}
```

### 预期结果

- `target-ftp/save_4.png` 创建成功且文件可以正常打开。
- 文件内容与 HTTP 响应内容一致。
- `input.close()` 被执行；HTTP 下载或 FTP 写入失败时，当前事件报错，不继续下游处理。

## 九、场景 8：对象和数组必须显式序列化

### 验证步骤

1. 将 `content` 直接设置为 JS 对象或数组，不设置 `contentType`。
2. 再将其改为 `JSON.stringify(content)` 后写入。

### 预期结果

- 直接传入对象/数组时，调用抛出异常，并提示使用 `JSON.stringify` 或显式二进制/流类型；不会写入 `[object Object]` 或数组的字符串表示。
- 显式序列化后按 UTF-8 写入，文件内容可被 JSON 解析。

## 十、场景 9：并发 `fail` 写入

### 验证步骤

使用两个并发事件向同一连接、同一路径执行：

```javascript
storage.update("target-ftp", {
  action: "write",
  target: { path: "out/concurrent.txt" },
  content: "value"
}, { overwrite: "fail" });
```

### 预期结果

- 只有一个事件返回 `status: "written"`。
- 另一个事件抛出目标已存在异常，不覆盖第一个事件写入的内容。

## 十一、场景 10：schema deduction 只读模式

### 预期结果

- `write`、`copy`、`delete` 分别返回兼容的 `written`、`copied`、`true` 结果。
- 不创建文件连接 executor，不执行远端写入、复制或删除。
- 请求结构、路径和内容类型仍会校验；对象/数组 text 内容仍需 `JSON.stringify`。

## 十二、场景 11：SFTP 验收

使用与 FTP 场景等价的 SFTP 连接，且部署 `tapdata-connectors#906` 或更高版本。

### 必测情况

- 目标父目录不存在：写入明确失败，不能产生半成品目标文件。
- 复制过程中断开 SFTP 数据/控制连接：当前事件失败，后续事件可以重新建连；源端故障不应销毁健康目标连接。
- 停止任务时存在进行中的读写：任务停止最终完成，不能永久等待原始流关闭。

### 预期结果

上述异常均不会把未完成的文件操作当成成功事件写入下游；恢复连接后新的事件可重新执行。

### 本轮验收记录

本地工作区没有可用的真实 SFTP 服务、连接配置或凭据，因此本轮未执行真实 SFTP 连接验收，不将以下结果标记为通过。离线验证已覆盖 SFTP 与 JS storage 共用的路径解析、回调式读写、目标长度校验、executor 故障隔离和非 FTP 延迟销毁逻辑；真实环境仍需在发布前按上述三项场景执行，并记录连接器版本、服务端地址类型和每项实际现象。
