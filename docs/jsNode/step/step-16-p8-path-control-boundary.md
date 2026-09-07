# TAP-12832 开发步骤 16：路径控制字符边界

## 本步骤目标

补齐文件路径安全边界，阻止换行、NUL 等控制字符进入公共文件复制 API 和共享路径策略，避免 FTP 命令、日志或 adapter 层出现歧义。

## 代码变更

- `FileCopyRequest` 在构造阶段拒绝 sourcePath/targetPath 中的控制字符。
- `FilePathPolicy` 对相对路径和 endpoint rootPath 使用统一的 `Character.isISOControl` 检查。
- 增加公共 API 控制字符测试，保留原有 `..`、绝对路径和协议 URL 测试。

## 验收结果

| 检查项 | 结果 |
| --- | --- |
| 公共 API 路径控制字符测试 | `mvn -q -pl plugin-kit/tapdata-api -Dtest=FileOperationApiTest test` 通过 |
| 共享服务路径和复制回归 | `mvn -q -pl connectors-common/file-connector-core -Dtest=FilePathPolicyTest,DefaultFileOperationServiceTest test` 通过 |
| 代码格式检查 | `git diff --check` 通过 |

## 边界说明

- 只允许相对文件路径；endpoint 的 `rootPath` 仍可为绝对远端目录，但禁止 `..`、协议 URL 和控制字符。
- 控制字符校验发生在 API 和共享服务两层，不能通过绕过某一层来放宽限制。
- 文件名中的普通 Unicode 字符（例如中文）不属于控制字符，仍可正常使用。
