# TAP-12615 DQL 开发进度索引

## 使用规则

- 开发严格按照 `doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md` 的步骤编号推进。
- 每个步骤完成后，在 `steps/` 下新增一份步骤总结，并同步开发计划中的状态。
- 每个里程碑满足退出条件后，在 `milestones/` 下新增一份里程碑总结。
- 步骤文档记录实际完成内容、设计决策、代码或文档产出、验证结果和后续依赖，不以“文件已创建”代替完成标准。
- Web 开发不在本计划范围内，只记录 TM API、权限码和错误码的契约交接。

## 步骤进度

| 步骤 | 状态 | 总结文档 |
| --- | --- | --- |
| A01 | 已完成 | `steps/A01-naming-contract.md` |
| A02 | 已完成 | `steps/A02-state-machine-contract.md` |
| A03 | 已完成 | `steps/A03-cross-component-contract.md` |
| A04 | 已完成 | `steps/A04-exception-classification-contract.md` |
| A05 | 已完成 | `steps/A05-configuration-and-poc-contract.md` |
| B01 | 已完成 | `steps/B01-domain-contract.md` |

## 里程碑进度

| 里程碑 | 状态 | 总结文档 |
| --- | --- | --- |
| M0 契约冻结 | 已完成 | `milestones/M0-contract-freeze.md` |
| M1 TM 基础能力 | 进行中 | 待生成 |
| M2 Engine 捕获闭环 | 未开始 | 待生成 |
| M3 TM 批次控制 | 未开始 | 待生成 |
| M4 Engine 回放闭环 | 未开始 | 待生成 |
| M5 告警与初始化 | 未开始 | 待生成 |
| M6 POC 验收 | 未开始 | 待生成 |
