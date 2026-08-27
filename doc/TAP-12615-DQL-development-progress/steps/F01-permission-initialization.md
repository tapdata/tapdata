# F01 权限初始化

## 1. 步骤结论

F01 已完成（待集成验证）。POC 采用单一 View 权限 `v2_exception_events`，页面资源为 `/exception-events`，不新增独立 Edit/Start 权限；事件详情和重处理操作继续由 TM 按事件所属任务的数据权限校验。

## 2. 实现内容

- 在 `init/idaas/4.22-7.json` 增加 `Permission` upsert：父权限为 `v2_advanced_features`，类型为 `read`，资源编码为 `v2_exception_events`。
- 增加管理员角色 `5b9a0a383fcba02649524bf1` 的 `RoleMapping` upsert，并设置 `self_only=false`。
- 使用 upsert 保持新环境初始化、旧环境升级和重复执行幂等。
- 新增 `DqlInitializationPatchTest`，校验权限、页面路径、角色映射和权限类型契约。

## 3. 验证结果

```bash
mvn -o -pl manager/tm -am \
  -Dtest=DqlInitializationPatchTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：`DqlInitializationPatchTest` 1/1 通过，TM 及依赖模块编译成功，`BUILD SUCCESS`。

## 4. 限制与后续依赖

本地没有可连接的 Mongo 初始化环境，因此尚未验证真实空库、已有权限和升级执行结果；重复执行及角色权限生效需在 F06/G02 中联调确认。Web 菜单和页面由 Web 侧交付，本步骤只冻结 TM 初始化契约。
