# TapData 审计事件目录

本目录与 `manager/tm-api/src/main/java/com/tapdata/tm/audit/AuditAction.java` 一一对应（TAP-13018，依据《TapData 审计日志增强正式产品需求文档》V1.0）。
新增或修改会改变配置、资源、权限、安全状态、任务状态、数据或外部系统的产品操作，以及高风险读取、下载、导出，发布前必须：

1. 在 `AuditAction` 登记动作（事件类型、对象类型、同步/异步、详情等级），并补齐 `auditMessages*.properties` 与前端三语文案；
2. 在接口上声明 `@Audited`，或在业务代码中通过 `AuditEventService` 显式记录；
3. 回答需求第 21 章发布准入清单；`AuditCatalogCoverageTest` 会拒绝未登记、也未列入排除清单的写接口。

规则摘要：

- 同步操作记录一条最终事件（成功/失败）；异步操作记录受理事件（已受理/失败），对象达到最终状态后由 SYSTEM 记录最终结果，两者共用 Operation ID。
- 请求在权限、参数或状态校验阶段失败时只记录一条失败事件。批量操作记录一条事件并保留对象级结果，混合结果为「部分成功」。
- 秘密字段只记录已设置、已替换、已清除或未变更；请求体与响应体不写入审计记录。
- 字段差异仅在能可靠取得前后值时提供；未取得时详情显示「该操作暂不提供字段级差异」。

| Action | 名称 | 事件类型 | 对象类型 | 同步/异步 | 详情等级 |
|---|---|---|---|---|---|
| `auth.login` | 登录 TapData | 认证 | session | 同步 | 基础审计 |
| `auth.logout` | 退出登录 | 认证 | session | 同步 | 基础审计 |
| `auth.session_terminate` | 终止会话 | 认证 | session | 同步 | 基础审计 |
| `auth.api_authenticate` | API 身份认证 | 认证 | credential | 同步 | 基础审计 |
| `access.denied` | 访问被拒绝 | 访问控制 | resource | 同步 | 基础审计 |
| `user.create` | 创建用户 | 用户 | user | 同步 | 基础审计 |
| `user.update` | 编辑用户 | 用户 | user | 同步 | 变更摘要 |
| `user.enable` | 启用用户 | 用户 | user | 同步 | 基础审计 |
| `user.disable` | 禁用用户 | 用户 | user | 同步 | 基础审计 |
| `user.delete` | 删除用户 | 用户 | user | 同步 | 基础审计 |
| `user.change_password` | 修改密码 | 用户 | user | 同步 | 基础审计 |
| `user.reset_password` | 重置密码 | 用户 | user | 同步 | 基础审计 |
| `user.reset_access_code` | 重置访问码 | 用户 | user | 同步 | 基础审计 |
| `user.view_access_code` | 查看或复制访问码 | 用户 | credential | 同步 | 基础审计 |
| `user.update_contact` | 修改联系方式 | 用户 | user | 同步 | 基础审计 |
| `user.update_tags` | 修改用户标签 | 用户 | user | 同步 | 基础审计 |
| `user.import` | 导入用户 | 用户 | user | 同步 | 变更摘要 |
| `user_group.create` | 创建用户组 | 用户 | user_group | 同步 | 基础审计 |
| `user_group.update` | 编辑用户组 | 用户 | user_group | 同步 | 变更摘要 |
| `user_group.delete` | 删除用户组 | 用户 | user_group | 同步 | 基础审计 |
| `role.create` | 创建角色 | 角色与权限 | role | 同步 | 基础审计 |
| `role.update` | 编辑角色 | 角色与权限 | role | 同步 | 变更摘要 |
| `role.delete` | 删除角色 | 角色与权限 | role | 同步 | 基础审计 |
| `role.grant` | 授予角色 | 角色与权限 | user | 同步 | 变更摘要 |
| `role.revoke` | 撤销角色 | 角色与权限 | user | 同步 | 变更摘要 |
| `permission.update` | 修改角色权限 | 角色与权限 | role | 同步 | 变更摘要 |
| `permission.data_update` | 修改数据权限 | 角色与权限 | resource | 同步 | 变更摘要 |
| `datasource.create` | 创建数据源 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.update` | 编辑数据源 | 数据源 | datasource | 同步 | 变更摘要 |
| `datasource.copy` | 复制数据源 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.delete` | 删除数据源 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.import` | 导入数据源 | 数据源 | datasource | 同步 | 变更摘要 |
| `datasource.export` | 导出数据源 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.test` | 测试数据源连接 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.update_tags` | 修改数据源标签 | 数据源 | datasource | 同步 | 基础审计 |
| `datasource.update_agent` | 修改数据源 Agent 设置 | 数据源 | datasource | 同步 | 变更摘要 |
| `external_storage.create` | 创建外部存储 | 外部存储 | external_storage | 同步 | 基础审计 |
| `external_storage.update` | 编辑外部存储 | 外部存储 | external_storage | 同步 | 变更摘要 |
| `external_storage.delete` | 删除外部存储 | 外部存储 | external_storage | 同步 | 基础审计 |
| `external_storage.test` | 测试外部存储连接 | 外部存储 | external_storage | 同步 | 基础审计 |
| `external_storage.set_default` | 设置默认外部存储 | 外部存储 | external_storage | 同步 | 基础审计 |
| `task.create` | 创建任务 | 任务 | task | 同步 | 基础审计 |
| `task.update` | 编辑任务 | 任务 | task | 同步 | 基础审计 |
| `task.rename` | 重命名任务 | 任务 | task | 同步 | 字段差异 |
| `task.copy` | 复制任务 | 任务 | task | 同步 | 基础审计 |
| `task.delete` | 删除任务 | 任务 | task | 异步 | 基础审计 |
| `task.start` | 启动任务 | 任务 | task | 异步 | 基础审计 |
| `task.stop` | 停止任务 | 任务 | task | 异步 | 基础审计 |
| `task.force_stop` | 强制停止任务 | 任务 | task | 异步 | 基础审计 |
| `task.reset` | 重置任务 | 任务 | task | 异步 | 基础审计 |
| `task.import` | 导入任务 | 任务 | task | 同步 | 变更摘要 |
| `task.export` | 导出任务 | 任务 | task | 同步 | 基础审计 |
| `task.update_tags` | 修改任务分类或标签 | 任务 | task | 同步 | 基础审计 |
| `task.update_alarm` | 修改任务告警 | 任务 | task | 同步 | 变更摘要 |
| `task.download_log` | 下载任务日志 | 任务 | task | 同步 | 基础审计 |
| `task.test_run` | 试运行处理节点 | 任务 | task | 同步 | 基础审计 |
| `data.preview` | 预览业务数据 | 任务 | table | 同步 | 基础审计 |
| `inspect.create` | 创建校验任务 | 数据校验 | inspect | 同步 | 基础审计 |
| `inspect.update` | 编辑校验任务 | 数据校验 | inspect | 同步 | 基础审计 |
| `inspect.delete` | 删除校验任务 | 数据校验 | inspect | 同步 | 基础审计 |
| `inspect.start` | 启动校验任务 | 数据校验 | inspect | 异步 | 基础审计 |
| `inspect.stop` | 停止校验任务 | 数据校验 | inspect | 异步 | 基础审计 |
| `inspect.import` | 导入校验任务 | 数据校验 | inspect | 同步 | 变更摘要 |
| `inspect.repair` | 执行数据修复 | 数据校验 | inspect | 同步 | 基础审计 |
| `inspect.export_result` | 下载校验差异或修复结果 | 数据校验 | inspect | 同步 | 基础审计 |
| `inspect.update_config` | 修改任务校验配置 | 数据校验 | task | 同步 | 基础审计 |
| `inspect.delete_result` | 删除校验结果 | 数据校验 | inspect | 同步 | 基础审计 |
| `log_cache.update` | 编辑 CDC Log Cache | CDC Log Cache | log_cache | 同步 | 基础审计 |
| `log_cache.update_config` | 修改 CDC Log Cache 配置 | CDC Log Cache | log_cache | 同步 | 字段差异 |
| `log_cache.create` | 创建 CDC Log Cache | CDC Log Cache | log_cache | 同步 | 基础审计 |
| `log_cache.stop_tables` | 停止表级挖掘 | CDC Log Cache | log_cache | 同步 | 变更摘要 |
| `log_cache.resume_tables` | 恢复表级挖掘 | CDC Log Cache | log_cache | 同步 | 变更摘要 |
| `log_cache.cancel_merge` | 取消 CDC Log Cache 合并 | CDC Log Cache | log_cache | 同步 | 基础审计 |
| `live_cache.create` | 创建 Live Cache | Live Cache | live_cache | 同步 | 基础审计 |
| `live_cache.update` | 编辑 Live Cache | Live Cache | live_cache | 同步 | 基础审计 |
| `live_cache.delete` | 删除 Live Cache | Live Cache | live_cache | 异步 | 基础审计 |
| `live_cache.copy` | 复制 Live Cache | Live Cache | live_cache | 同步 | 基础审计 |
| `api_app.create` | 创建 API 应用 | 数据 API | api_app | 同步 | 基础审计 |
| `api_app.update` | 编辑 API 应用 | 数据 API | api_app | 同步 | 基础审计 |
| `api_app.delete` | 删除 API 应用 | 数据 API | api_app | 同步 | 基础审计 |
| `api.create` | 创建 API 服务 | 数据 API | api | 同步 | 基础审计 |
| `api.update` | 编辑 API 服务 | 数据 API | api | 同步 | 基础审计 |
| `api.copy` | 复制 API 服务 | 数据 API | api | 同步 | 基础审计 |
| `api.delete` | 删除 API 服务 | 数据 API | api | 同步 | 基础审计 |
| `api.publish` | 发布 API 服务 | 数据 API | api | 异步 | 基础审计 |
| `api.unpublish` | 下线 API 服务 | 数据 API | api | 异步 | 基础审计 |
| `api.import` | 导入 API 服务 | 数据 API | api | 同步 | 变更摘要 |
| `api.export` | 导出 API 服务 | 数据 API | api | 同步 | 基础审计 |
| `api.update_security` | 修改 API 安全配置 | 数据 API | api | 同步 | 变更摘要 |
| `api.debug` | 调试 API | 数据 API | api | 同步 | 基础审计 |
| `api.export_doc` | 导出 API 文档 | 数据 API | api | 同步 | 基础审计 |
| `api_client.create` | 创建 API 客户端 | 数据 API | api_client | 同步 | 基础审计 |
| `api_client.update` | 编辑 API 客户端 | 数据 API | api_client | 同步 | 变更摘要 |
| `api_client.delete` | 删除 API 客户端 | 数据 API | api_client | 同步 | 基础审计 |
| `api_client.view_secret` | 查看或复制客户端密钥 | 数据 API | credential | 同步 | 基础审计 |
| `settings.update` | 修改系统设置 | 系统设置 | settings | 同步 | 字段差异 |
| `settings.test` | 测试系统设置 | 系统设置 | settings | 同步 | 基础审计 |
| `audit.retention_update` | 修改审计日志保留时间 | 审计 | settings | 同步 | 字段差异 |
| `sso.update` | 修改 SSO 配置 | 系统设置 | sso_config | 同步 | 变更摘要 |
| `sso.test` | 测试 SSO 配置 | 系统设置 | sso_config | 同步 | 基础审计 |
| `ldap.test` | 测试 LDAP 连接 | 系统设置 | ldap_config | 同步 | 基础审计 |
| `alarm.rule_update` | 修改告警规则 | 通知告警 | alarm_rule | 同步 | 基础审计 |
| `alarm.receiver_update` | 修改告警接收人 | 通知告警 | alarm_rule | 同步 | 变更摘要 |
| `alarm.close` | 关闭告警 | 通知告警 | alarm | 同步 | 基础审计 |
| `webhook.create` | 创建 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `webhook.update` | 编辑 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `webhook.enable` | 启用 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `webhook.disable` | 停用 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `webhook.delete` | 删除 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `webhook.test` | 测试 Webhook | 通知告警 | webhook | 同步 | 基础审计 |
| `notification.update` | 修改通知设置 | 通知告警 | notification | 同步 | 基础审计 |
| `license.import` | 导入 License | License | license | 同步 | 基础审计 |
| `connector.install` | 安装连接器 | 连接器 | connector | 同步 | 基础审计 |
| `connector.update` | 修改连接器 | 连接器 | connector | 同步 | 基础审计 |
| `connector.delete` | 删除连接器 | 连接器 | connector | 同步 | 基础审计 |
| `customer.update` | 修改客户信息 | 系统设置 | customer | 同步 | 基础审计 |
| `message.read` | 标记通知为已读 | 通知消息 | message | 同步 | 基础审计 |
| `message.delete` | 删除通知 | 通知消息 | message | 同步 | 基础审计 |
| `service.start` | 启动服务 | 服务生命周期 | service | 异步 | 基础审计 |
| `service.stop` | 停止服务 | 服务生命周期 | service | 异步 | 基础审计 |
| `service.restart` | 重启服务 | 服务生命周期 | service | 异步 | 基础审计 |
| `service.upgrade` | 升级服务 | 服务生命周期 | service | 异步 | 基础审计 |
| `service.start_failed` | 服务启动失败 | 服务生命周期 | service | 同步 | 基础审计 |
| `service.abnormal_stop` | 服务异常停止 | 服务生命周期 | service | 同步 | 基础审计 |
| `service.disconnect` | 服务失联 | 服务生命周期 | service | 同步 | 基础审计 |
| `service.reconnect` | 服务恢复连接 | 服务生命周期 | service | 同步 | 基础审计 |
| `agent.create` | 创建 Agent | 集群与 Agent | agent | 同步 | 基础审计 |
| `agent.delete` | 删除 Agent | 集群与 Agent | agent | 同步 | 基础审计 |
| `agent.unbind` | 解绑 Agent | 集群与 Agent | agent | 同步 | 基础审计 |
| `agent_group.update` | 修改 Agent 分组 | 集群与 Agent | agent_group | 同步 | 变更摘要 |
| `agent_group.delete` | 删除 Agent 分组 | 集群与 Agent | agent_group | 同步 | 基础审计 |
| `cluster.node_remove` | 移除集群节点 | 集群与 Agent | service | 同步 | 基础审计 |
| `cluster.monitor_update` | 修改集群监控项 | 集群与 Agent | service | 同步 | 基础审计 |
| `audit.export` | 导出审计日志 | 审计 | audit_log | 同步 | 基础审计 |
| `audit.access_denied` | 访问审计资源失败 | 审计 | audit_log | 同步 | 基础审计 |

## 不纳入审计的接口

- 页面浏览、查询、筛选、分页、刷新等无副作用交互（需求 8.2）；正常查看审计日志。
- 引擎、API Server、Agent 的内部上报与回调（心跳、任务状态回报、转换结果、指标与日志上报、通用 `update`/`upsertWithWhere` 回写），其中服务启停与失联由 SYSTEM 生命周期事件覆盖。
- Workflow 相关操作（需求 5.2）；已发布数据 API 的逐次业务调用（由 API Server 调用审计负责）。
