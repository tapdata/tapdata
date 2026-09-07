# TAP-12832 开发步骤总结：P1 公共文件操作 API

## 本步骤完成内容

在 `tapdata-common-lib/plugin-kit/tapdata-api` 增加公共文件操作契约：

- `FileEndpoint`：协议、参数和 rootPath，构造时复制并冻结参数 Map。
- `FileCopyRequest`：source/target、相对路径、覆盖策略、校验方式、重试、超时和 dry-run。
- `FileOperationResult`、`FileBatchResult`：状态、路径、字节数、校验摘要、尝试次数和耗时。
- `FileVerifyMode`、`FileOperationStatus`、`FileStorageCapability`。
- `FileOperationErrorCode`、`FileOperationException`。
- `FileMetadata`、`FileListRequest`、`FileAccess`、`FileValidationResult`。
- `TapFileOperationService` 和 `FileOperationServiceProvider` 接口。

## 关键实现决策

1. API 模块不依赖 FTPClient、Spring 或具体 connector 实现。
2. `FileCopyRequest` 默认 `overwrite=false`、`verifyMode=SIZE`、`retryTimes=0`、`timeoutMs=120000`。
3. endpoint 参数在构造时防御性复制，结果列表不可变。
4. 请求构造阶段拒绝空路径、绝对路径、协议 URL 和 `..` 路径段；更细的 rootPath 策略由共享 service 继续校验。
5. provider 只负责提供 service，classloader 和 session 生命周期由后续 `file-connector-core` 实现。

## 测试与自审

新增 `FileOperationApiTest`，覆盖：

- endpoint 参数防御性复制和 request 默认值。
- 缺少 endpoint/path 和危险相对路径时拒绝构造。
- batch result 列表不可变、计数和状态读取。

执行结果：

- 先运行失败测试，确认因 DTO 尚不存在导致编译失败。
- 实现后运行 `mvn -q -Dtest=FileOperationApiTest test`：通过。
- 运行 `tapdata-api` 模块完整 `mvn -q test`：通过。现有测试有大量历史日志输出，但 Maven 退出码为 0。

## 下一步

执行 P2：先补 `TapFileStorage` capability 和 FTP adapter 的失败测试，再修复写入结果、rename、目录创建和 session 安全边界。
