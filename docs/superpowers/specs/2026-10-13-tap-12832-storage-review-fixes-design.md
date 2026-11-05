# TAP-12832 第五轮评审问题修复设计

## 背景与目标

PR #3478 为增强 JS 节点增加了按连接名访问文件存储的能力。`ply0011` 第五轮评审重新打开了 11 个代码线程，涉及推演副作用、executor 缓存失效、并发覆盖、文本类型误写、Locale、FTP 生命周期、复制完整性、锁泄漏、无效往返和 PDK 资源生命周期。

目标是在不改变 JS API 形状、普通运行的真实文件操作语义、`skip/overwrite/fail` 使用规则和现有错误传播规则的前提下，修复这些实际缺陷，并用回归测试证明修复没有破坏既有功能。

明确不在本次改变的行为：

- 普通任务中 `storage.write/copy/delete` 仍然真实操作远端文件。
- `STREAM` 输入流仍由 JS 脚本负责关闭；引擎不接管调用方所有权。
- 单次远端操作失败仍返回原始失败，不在同一次 JS 调用中隐式重放写入或复制。
- 路径安全校验、根目录拒绝和单根路径规则保持不变。

## 设计

### 1. 推演/预览的 storage 结果形状

`StorageFacade.readOnly` 继续保证推演阶段不创建 executor、不登录或访问远端文件。它只完成必要的请求形状校验，然后返回普通成功调用会返回的结构：

- `write` 返回 `{status: "written", targetPath: ...}`；
- `copy` 返回 `{status: "copied", targetPath: ...}`；
- `delete` 返回 `true`。

这样增强 JS 可以继续把结果字段写入记录并生成下游模型；不引入 `skipped` 等新状态。普通 facade 的 `result` 仍要求真实 `saveFile` 返回非 null，避免把未写入误报为成功。推演跳过行为记录一条 debug/info 日志，不改变返回值。

### 2. executor 失效的身份安全与源/目标隔离

`StorageExecutorsManager.invalidate` 改为接收发生异常的 `StorageExecutor`，只在缓存中的 future 仍解析到同一个 executor 时执行 compare-and-remove 和关闭。旧 executor 的迟到异常不能关闭已经重建的健康 executor。

`StorageFacade` 不再用一个覆盖整个 `update` 的兜底 catch 把 copy 的两侧一起失效：

- 获取/读取源 executor 或源文件失败，只失效源 executor；
- 目标存在性检查或保存失败，只失效目标 executor；
- 同连接复制时只执行一次对应 executor 的失效。

输入校验、目标已存在、源为目录等 `StorageOperationException` 仍不触发失效。

### 3. 并发写入的原子检查与保存

在 `StorageExecutor` 中增加一个受 executor 锁保护的操作边界，默认实现适配现有 `TapFileStorage`，`PdkStorageExecutor` 实现使用 `PdkManagedFileStorage` 的同一把操作锁。锁使用可重入实现，以便组合操作内部继续调用受管 storage 方法。

`StorageFacade.write` 对 `fail/skip` 在同一个锁保护的回调内完成存在性检查和保存；保存时传入 `canReplace=false`。这样同一 executor 内的并发调用不会同时通过检查后互相覆盖，`fail` 在竞争发生时仍抛异常，`skip` 返回 `reused`。`overwrite` 传入 `canReplace=true`，并跳过 facade 侧无效的预检查。

`copy` 对 `fail/skip` 保留可选的快速预检查，但在读取临时文件完成后再次在目标锁内检查并保存，处理预检查到写入之间的竞争。跨存储复制仍先释放源锁，再获取目标锁，避免反向复制死锁。

### 4. 复制完整性

源文件元数据有可靠长度时，写入目标前比较临时文件实际长度与源文件长度；不一致抛 `StorageOperationException`，不调用目标 `saveFile`，也不返回 `copied`。长度未知时保留 connector 自身的完整性判断，不凭空拒绝合法复制。

### 5. 内容类型与 Locale

text 模式拒绝 `Map`、`List` 及其他 JS 对象/数组形态，提示显式使用 `JSON.stringify`；字符串、字符序列等已有文本输入继续按 UTF-8 写入。动作名、overwrite 模式和协议名均使用 `toLowerCase(Locale.ROOT)`，消除 Turkish locale 等环境相关解析错误。

### 6. FTP 关闭与 raw stream API

`PdkManagedFileStorage` 等待操作锁最多 5 秒。FTP connector 的 destroy 已具备中断正在进行的 I/O 的生命周期语义，因此 FTP 超时后直接调用一次 delegate destroy 并释放 PDK 资源；其他协议仍等待操作结束后后台回收，避免把不具备并发 destroy 保证的 connector 强行关闭。

实现 `TapFileStorage.readFile(String)` 与 `openFileOutputStream` 的受管 wrapper 目前没有 facade 调用方，且调用方不关闭流会永久持有操作锁。两者改为明确抛出 `UnsupportedOperationException`，删除持锁流包装；facade 使用的 callback 读取和 `saveFile` 路径保持不变。

### 7. PDK 生命周期重复代码

本次先抽取只承载 storage executor 所需的 node 资源释放逻辑，确保 `associateId`、state map 和 table map 的释放仍在创建失败、普通 close、超时 destroy 三条路径上最多执行一次。只有在不改变现有 connector 初始化/停止语义的情况下，才与 `ScriptExecutorsManager` 共享 holder；如果需要改变 connector stop 顺序，则保留现有脚本执行器实现，并把 storage holder 的边界限制在本次修复范围内。

### 8. 文档与验收

同步产品手册说明：推演/预览中的 storage 调用返回兼容结果但不会远端执行；text 对象/数组必须显式序列化；`fail/skip` 的并发语义由同 executor 锁保证。验收手册增加 SFTP 场景，至少覆盖父目录不存在、传输中途断开和任务停止，并注明 FTP 数据通道/截断保护依赖 `tapdata-connectors#906` 之后版本，发布时必须配套升级。

## 错误处理与测试策略

每个行为修复先添加能复现旧行为的失败测试，再实现最小修复。测试覆盖：

1. read-only facade 不创建 executor 且返回三种兼容结果；
2. 迟到失败不会关闭新 executor，copy 源故障不会重建/销毁健康目标；
3. fail 并发竞争不会覆盖，overwrite 不做 facade 预检查；
4. text 对象/数组拒绝、Locale.ROOT 解析；
5. 截断临时文件不会保存；
6. FTP 超时触发受控 destroy，非 FTP 仍延迟，raw stream API 明确拒绝；
7. 既有 write/copy/find/exists/delete、路径校验和 manager 生命周期测试继续通过。

真实 FTP/SFTP 服务验收不能由 fake storage 单测替代；代码测试完成后按验收手册记录环境验收要求和连接器版本依赖。
