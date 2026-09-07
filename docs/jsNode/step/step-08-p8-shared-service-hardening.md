# TAP-12832 P8：共享文件服务集成加固

## 完成内容

1. 将 `file-connector-core` 和 FTP storage 对 `tapdata-api` 的显式依赖统一到当前 `2.0.10-SNAPSHOT`，避免公共 API 与 connector 使用不同版本。
2. 加固 `DefaultFileOperationService.copy`：
   - 按 `retryTimes` 重试可恢复的连接、远端 IO 和临时文件写入错误。
   - 按 `timeoutMs` 检查整个复制过程，超时返回 `FILE_TIMEOUT`。
   - 使用 SHA-256 支持 `CHECKSUM` 校验和 `expectedChecksum` 校验。
   - `CHECKSUM` 模式下只有源、目标内容一致才返回 `REUSED`；`NONE` 模式不把同名文件误判为复用。
   - 临时目录和目标父目录使用 endpoint root 解析，并在 adapter 声明 `MAKE_DIRECTORY` 时递归创建。
   - 结果保留最终校验摘要和实际尝试次数。
3. 补充共享服务单元测试：同大小不同内容的 checksum 冲突、checksum 成功、一次写失败后的重试和 attempts 统计。

## 自主审核与验证

- `mvn -q -pl connectors-common/file-connector-core -Dtest=DefaultFileOperationServiceTest test`：通过。
- `mvn -q -pl connectors-common/file-connector-core,file-storages/ftp-file -am test`：通过。
- `git diff --check`：通过。

## 边界说明

- 超时检查发生在读写循环和阶段边界；底层 adapter 的单次阻塞调用由其自身连接/数据超时控制，服务不会创建无法回收的后台线程。
- checksum 使用共享服务的流式读取，不要求把文件正文放入 JS 或内存大对象；目标协议若没有原生 checksum 能力仍可通过流式 SHA-256 校验。
- `copyBatch` 仍按顺序执行，任一单文件异常立即失败；没有跨文件分布式回滚，也不会删除此前已经发布的正式文件。
