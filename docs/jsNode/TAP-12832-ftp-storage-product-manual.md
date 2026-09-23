# Enhanced JS `storage` 文件操作 API

## 1. 使用范围

`storage` 仅注入增强 JS 节点，标准 JS 节点不提供该对象。

文件连接只需要在连接管理中创建，连接不需要放入当前任务 DAG。调用参数使用连接名称，不需要在 JS 中填写账号、密码或连接配置。

```javascript
storage.update("target-ftp", data, options);
```

方法为同步调用。文件操作成功后方法返回；发生文件或连接错误时抛出异常。

## 2. 方法列表

| 方法 | 作用 | 返回值 |
| --- | --- | --- |
| `update(connectionName, data, options)` | 写入文件或复制文件 | 结果对象 |
| `find(connectionName, query, options)` | 查询文件元数据 | 元数据对象；不存在时为 `null` |
| `exists(connectionName, path)` | 判断文件是否存在 | `boolean` |
| `delete(connectionName, data, options)` | 删除文件 | `boolean` |

## 3. `storage.update`

### 3.1 参数

```javascript
storage.update(connectionName, data, options)
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `connectionName` | `String` | 目标文件连接名称 |
| `data` | `Object` | 文件操作内容；`action` 可选，默认值为 `write` |
| `options` | `Object` | 操作选项，可传 `null` 或 `{}` |

### 3.2 写文件

`data.action` 为 `write` 时，使用 `target.path` 指定目标文件，使用 `contentType` 指定内容处理方式。未传 `contentType` 时按 `text` 处理。

#### 3.2.1 常规写方式

常规文本或 JSON 文件使用默认的 `text` 类型，内容按 UTF-8 写入：

```javascript
storage.update("target-ftp", {
  action: "write",
  target: {
    path: "out/result.json"
  },
  content: JSON.stringify(record)
}, {
  overwrite: "overwrite"
});
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `action` | `String` | 固定为 `write` |
| `target.path` | `String` | 目标文件路径 |
| `contentType` | `String` | 内容处理类型；未传时默认为 `text`，忽略大小写 |
| `content` | `String` 或可转字符串的值 | 文件内容，按 UTF-8 写入 |

`contentType: "text"` 适用于文本、JSON、CSV 等字符内容。未传 `contentType` 时不会根据 `content` 的实际类型自动推断为二进制。

#### 3.2.2 HTTP 二进制文件同步

HTTP 图片、压缩包等二进制文件不能按默认 `text` 类型写入，需要先通过 `httpUtil` 获取二进制内容，再根据数据形态选择 `bytes` 或 `stream`。

##### bytes 方式

`httpUtil.downloadBytes(url)` 将 HTTP 响应完整读取为 Java `byte[]`。`contentType: "bytes"` 会按原始字节写入，不进行字符集转换。该方式会将整个文件加载到内存，适合文件大小可控的场景：

```javascript
function process(record) {
  var content = httpUtil.downloadBytes(record.url);

  storage.update("target-ftp", {
    action: "write",
    contentType: "bytes",
    target: {
      path: "save_" + record.id + ".png"
    },
    content: content
  }, {
    overwrite: "overwrite"
  });

  return record;
}
```

##### stream 方式

`httpUtil.openStream(url)` 返回 HTTP 响应的 Java `InputStream`。`contentType: "stream"` 会以流方式写入，适合不希望一次性加载完整文件的场景。

`storage.update` 不会自动关闭 `stream` 内容，JS 脚本必须在使用完成后关闭输入流，建议使用 `try/finally`：

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
      throw new Error("HTTP stream FTP write failed");
    }
  } finally {
    input.close();
  }

  return record;
}
```

| 类型 | `content` 类型 | 适用场景 | 资源要求 |
| --- | --- | --- | --- |
| `bytes` | Java `byte[]` 或 JS 数字数组/List | 文件大小可控、需要完整获取内容后再写入 | 不需要 JS 关闭 |
| `stream` | Java `InputStream` | 大文件或希望流式传输 | JS 必须在 `finally` 中调用 `close()` |

类型名称忽略大小写，例如 `BYTES`、`Bytes` 和 `bytes` 等价。未知类型会抛出异常。MIME 类型（例如 `image/png`）不能作为 `contentType`。

`contentType` 只用于 `action: "write"`。`action: "copy"` 直接在源文件连接和目标文件连接之间复制，不读取 `content`。

### 3.3 复制文件

`data.action` 为 `copy` 时，`connectionName` 是目标连接，`source.connection` 是源连接：

```javascript
storage.update("target-ftp", {
  action: "copy",
  source: {
    connection: "Source-ftp",
    path: "in/a.txt"
  },
  target: {
    path: "out/a.txt"
  }
}, {
  overwrite: "overwrite"
});
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `action` | `String` | 固定为 `copy` |
| `source.connection` | `String` | 源文件连接名称 |
| `source.path` | `String` | 源文件路径 |
| `target.path` | `String` | 目标文件路径 |

源连接和目标连接可以相同，也可以不同。

### 3.4 `options.overwrite`

| 值 | 含义 |
| --- | --- |
| `skip` | 目标文件存在时不覆盖，返回 `reused`；默认值 |
| `overwrite` | 覆盖目标文件，写入返回 `written`，复制返回 `copied` |
| `fail` | 目标文件存在时抛出异常 |

### 3.5 返回值

```javascript
{
  status: "copied",
  targetPath: "out/a.txt",
  bytes: 1024
}
```

| 字段 | 含义 |
| --- | --- |
| `status` | `written`、`copied` 或 `reused` |
| `targetPath` | 实际目标文件路径 |
| `bytes` | 目标文件大小；底层存储返回文件信息时提供 |

## 4. `storage.find`

```javascript
var file = storage.find("Source-ftp", {
  path: "in/a.txt"
}, null);
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `connectionName` | `String` | 文件连接名称 |
| `query.path` | `String` | 文件或目录路径 |
| `options` | `Object` | 当前无额外选项，可传 `null` 或 `{}` |

返回值：

```javascript
{
  path: "in/a.txt",
  length: 1024,
  size: 1024,
  lastModified: 1710000000000,
  directory: false
}
```

文件不存在时返回 `null`。

## 5. `storage.exists`

```javascript
var exists = storage.exists("Source-ftp", "in/a.txt");
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `connectionName` | `String` | 文件连接名称 |
| `path` | `String` | 文件路径 |

返回文件是否存在；目录不会作为文件返回 `true`。

## 6. `storage.delete`

```javascript
var deleted = storage.delete("Source-ftp", {
  path: "in/a.txt"
}, null);
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `connectionName` | `String` | 文件连接名称 |
| `data.path` | `String` | 待删除文件或目录路径 |
| `options` | `Object` | 当前无额外选项，可传 `null` 或 `{}` |

返回底层文件存储的删除结果。

## 7. 路径规则

- 路径相对于文件连接配置的根目录。
- 支持使用 `/` 开头的路径；系统会进行统一处理。
- 不支持包含 `..`、控制字符或协议 URL 的路径。
- `connectionName` 必须是连接管理中已存在的文件连接名称。

## 8. 功能边界

### 支持

- 增强 JS 中按连接名称操作文件连接。
- FTP 文件写入、单文件复制、查询、存在性判断和删除。
- FTP 到 FTP、文件连接到文件连接的单文件复制。
- 按事件逻辑决定是否执行文件操作。
- `skip`、`overwrite`、`fail` 三种目标文件处理方式。
- 使用 `httpUtil.downloadBytes` 获取 HTTP 二进制内容后，通过 `contentType: "bytes"` 写入文件连接。
- 使用 `httpUtil.openStream` 获取 HTTP 输入流后，通过 `contentType: "stream"` 写入文件连接；输入流由脚本关闭。

当前首要验证协议为 FTP。引擎已包含 `local`、`ftp`、`sftp`、`smb`、`s3fs`、`nfs`、`oss` 的现有 storage 映射；其他协议是否可用取决于对应连接器、PDK 和连接配置，需要单独验证。

### 不支持

- 在标准 JS 节点中使用 `storage`。
- 通过 `ScriptExecutorsManager.getScriptExecutor("target-ftp")` 执行 FTP 文件操作。
- 目录列表、递归扫描、批量复制、文件移动/重命名、创建目录和追加写入 API。
- checksum 校验、自动重试、`dryRun`、原子发布和跨重启业务幂等。
- 将 FTP client、PDK 对象或连接凭据暴露给 JS；`InputStream` 只能由受控 helper（如 `httpUtil.openStream`）返回。
- 将 URL、HTTP 响应对象或普通字符串自动识别为文件二进制内容；HTTP 内容必须由脚本先通过 `httpUtil.downloadBytes` 或 `httpUtil.openStream` 获取，再以 `bytes` 或 `stream` 类型传入。
- 将 MIME 类型（例如 `image/png`）作为 `contentType`；`contentType` 表示内容处理方式，不表示文件 MIME 类型。
- FTP 操作与下游数据库写入之间的跨系统事务回滚。FTP 成功后下游数据库仍可能因自身原因写入失败。

## 9. `httpUtil` 文件下载方法

`httpUtil` 在增强 JS 节点中提供 HTTP/HTTPS GET 下载能力，支持自动跟随 30x 重定向。下载失败或 HTTP 返回非成功状态时抛出异常，不自动重试。

| 方法 | 返回值 | 适用场景 | 资源要求 |
| --- | --- | --- | --- |
| `httpUtil.downloadBytes(url)` | Java `byte[]` | 文件大小可控，需要以完整字节数组处理 | 不需要 JS 关闭；文件内容会完整加载到内存 |
| `httpUtil.openStream(url)` | Java `InputStream` | 大文件或需要流式传输 | JS 使用完成后必须调用 `close()` |

### 9.1 `httpUtil.downloadBytes`

```javascript
var content = httpUtil.downloadBytes(record.url);
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `url` | `String` | HTTP 或 HTTPS 文件地址 |

返回完整的 Java `byte[]`。传给 `storage.update` 时必须设置 `contentType: "bytes"`，否则未传 `contentType` 会按默认 `text` 处理。

### 9.2 `httpUtil.openStream`

```javascript
var input = httpUtil.openStream(record.url);
try {
  storage.update("target-ftp", {
    action: "write",
    contentType: "stream",
    target: { path: "save_" + record.id + ".png" },
    content: input
  }, { overwrite: "overwrite" });
} finally {
  input.close();
}
```

| 参数 | 类型 | 含义 |
| --- | --- | --- |
| `url` | `String` | HTTP 或 HTTPS 文件地址 |

返回 HTTP 响应体 `InputStream`。该流由 JS 脚本负责关闭，建议始终使用 `try/finally`，即使 FTP 写入失败也必须释放流。

### 9.3 使用说明

- `downloadBytes` 适合中小文件或需要先获取完整内容的逻辑，但文件大小受 JS 节点可用内存影响。
- `openStream` 适合大文件流式写入；只能消费一次，关闭后不能继续读取或重复传给 `storage.update`。
- 两个方法只负责下载，不负责根据 URL 后缀设置文件名或 MIME 类型；目标路径和文件扩展名由 JS 代码指定。
- HTTP 下载和 FTP 写入不是跨系统事务；下载或写入异常会抛出，是否继续处理由任务错误处理策略决定。
