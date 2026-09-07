# TAP-12832 开发步骤 15：文件能力发布开关

## 本步骤目标

为 JS 节点文件能力增加可回滚的 JVM 开关。默认开启以保持已实现功能可用；设置为 `false` 时，带文件配置的节点在初始化阶段拒绝建立共享文件服务，不创建远端 session。

## 代码变更

- 在 `HazelcastJavaScriptProcessorNode` 增加属性 `tapdata.js.file-operation.enabled`。
- 文件配置检测通过后先检查开关，再调用 `FileOperationServiceLoader`。
- 关闭时返回稳定的 `FILE_SERVICE_UNAVAILABLE` 文件错误码，不暴露连接参数。
- 清理重复的空参数判断，并增加大小写不敏感的开关解析测试。

## 验收结果

| 检查项 | 结果 |
| --- | --- |
| 未配置属性时默认开启 | 通过单元测试 |
| 设置 `false` 时关闭 | 通过单元测试 |
| `TRUE` 等大小写值可开启 | 通过单元测试 |
| iengine-common 更新构造函数安装 | `mvn -q -pl iengine/iengine-common -DskipTests install` 通过 |
| iengine-app 目标测试 | 本仓库已有 `ObsLogger.alert`、ShareCDC 构造函数/错误码等基线编译错误，未进入测试执行；本步骤新增构造函数错误已通过安装最新 iengine-common 消除 |

## 使用和边界

- 属性名：`tapdata.js.file-operation.enabled`；缺省或非 `false` 值均视为开启。
- 关闭开关只阻止带文件配置的 JS 节点；没有文件配置的旧 JS 节点不受影响。
- 开关在节点初始化时读取，修改后需重新创建/重启相关引擎节点才会生效。
