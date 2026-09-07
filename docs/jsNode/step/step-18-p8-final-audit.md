# TAP-12832 开发步骤 18：P8 最终自审与回归矩阵

## 本步骤目标

对公共 API、共享文件服务、TM 参数模型/密文、引擎访问器与 facade、前端编辑器进行最终自审，确认每个代码提交均有对应步骤文档和本地 commit，并记录当前仓库的基线阻塞。

## 已完成的可执行回归

| 模块 | 命令 | 结果 |
| --- | --- | --- |
| tapdata-api | `mvn -q -pl plugin-kit/tapdata-api -Dtest=FileOperationApiTest,TapFileStorageCapabilityTest test` | 通过 |
| file-connector-core + FTP | `mvn -q -pl connectors-common/file-connector-core,file-storages/ftp-file -am test` | 通过 |
| TM 密文工具 | `mvn -q -pl manager/tm-api -Dtest=JsNodeConfigSecretUtilTest test` | 通过 |
| TM commons 参数模型 | `mvn -q -pl manager/tm-common -Dtest=JsNodeConfigParamTest test` | 通过 |
| iengine-common | `mvn -q -pl iengine/iengine-common -Dtest=JsNodeConfigAccessorTest,FileScriptExecutorTest,ScriptUtilTest test` | 通过 |
| 前端参数工具 | `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js` | 5 项通过 |
| 前端格式 | `pnpm exec prettier --check ...` | 通过 |

## 自审结论

- JS 脚本配置访问器名称固定为 `jsNodeConfig`，没有注入通用 `config`。
- FTP/file 操作复用 `TapFileStorage`、`FileStorageFactory`、`FileServiceConfigMapper` 和 `DefaultFileOperationService`，引擎没有新增 FTP 客户端实现。
- 配置在 JS 节点内以 `scriptParams` 持久化，可自由增加/删除参数，支持类型、描述和加密标记。
- 加密参数由 TM 在任务 DAG 写入和独立 DAG 更新路径幂等加密；正式任务才解密，试运行强制 dry-run。
- 文件成功结果为 `COPIED`/`REUSED` 才允许当前业务事件继续；文件能力节点关闭记录级并发，不建立跨业务事件 barrier。
- 临时文件发布、size/checksum 校验、重试、session 隔离、路径越界和控制字符校验均有实现或单测覆盖。
- JVM 属性 `tapdata.js.file-operation.enabled=false` 可阻断文件 service 初始化，便于灰度回滚。

## 当前环境限制

- `manager/tm` 的目标测试仍被仓库已有 SSO 源码缺失阻断（`com.tapdata.tm.sso.*` 多个类无法解析），未归因于本次变更；保存前加密测试已保留，待基线恢复后执行。
- `iengine-app` 目标测试仍被已有 `ObsLogger.alert`、ShareCDC 构造函数和错误码不匹配阻断；本次新增 `FileScriptExecutor` 三参数构造函数问题已通过安装最新 `iengine-common` 快照消除。
- 本地未启动真实 FTP/SFTP/SMB 服务；共享 service 使用 fake storage 覆盖传输、校验、重试、dry-run 和冲突逻辑，真实协议连通性仍需预发环境验收。

## 交付边界

本步骤完成后代码和文档已提交到各自本地分支，没有执行远端 push。最终 FTP 账号权限、临时目录/rename 能力、业务路径字段和目标库幂等性由用户在预发或验收环境验证。
