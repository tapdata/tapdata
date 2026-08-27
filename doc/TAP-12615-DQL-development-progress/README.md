# TAP-12615 DQL 开发进度索引

## 使用规则

- 开发严格按照 `doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md` 的步骤编号推进。
- 每个步骤完成后，在 `steps/` 下新增一份步骤总结，并同步开发计划中的状态。
- 每个代码开发步骤独立创建一笔以 `feat(TAP-12615): ` 开头的本地 commit，不执行 push。
- 每个步骤独立提交并保留 review checkpoint；若用户已明确授权连续开发，则提交后自动进入下一步骤，仍按步骤记录和验证。
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
| B02 | 已完成 | `steps/B02-event-repository.md` |
| B03 | 已完成 | `steps/B03-batch-repository.md` |
| B04 | 已完成 | `steps/B04-ttl-lifecycle.md` |
| B05 | 已完成 | `steps/B05-ttl-index-initialization.md` |
| B06 | 已完成 | `steps/B06-report-validation-and-safety.md` |
| B07 | 已完成 | `steps/B07-event-identity-and-deduplication.md` |
| B08 | 已完成 | `steps/B08-engine-report-api.md` |
| B09 | 已完成 | `steps/B09-query-api.md` |
| B10 | 已完成 | `steps/B10-task-data-permission.md` |
| B11 | 已完成 | `steps/B11-error-semantics-and-api-doc.md` |
| B12 | 已完成 | `steps/B12-tm-foundation-regression.md` |

### 阶段 C：Engine 捕获与分层路由

| 步骤 | 状态 | 总结文档 |
| --- | --- | --- |
| C01 | 已完成 | `steps/C01-engine-dql-model-and-tm-client.md` |
| C02 | 已完成 | `steps/C02-engine-payload-serialization.md` |
| C03 | 已完成 | `steps/C03-engine-preview-and-identity.md` |
| C04 | 已完成 | `steps/C04-engine-exception-classifier.md` |
| C05 | 已完成 | `steps/C05-engine-storm-guard.md` |
| C06 | 已完成 | `steps/C06-engine-event-reporter.md` |
| C07 | 已完成 | `steps/C07-engine-target-write-capture.md` |
| C08 | 已完成 | `steps/C08-engine-process-capture.md` |
| C09 | 已完成 | `steps/C09-engine-js-custom-process-capture.md` |
| C10 | 已完成 | `steps/C10-engine-later-success-callback.md` |
| C11 | 已完成 | `steps/C11-engine-skip-metric-and-error-semantics.md` |
| C12 | 已完成 | `steps/C12-engine-capture-regression.md` |

### 阶段 D：TM 重处理批次与补偿

| 步骤 | 状态 | 总结文档 |
| --- | --- | --- |
| D01 | 已完成 | `steps/D01-recovery-preview.md` |
| D02 | 已完成 | `steps/D02-recovery-order.md` |
| D03 | 已完成 | `steps/D03-recovery-task-lock.md` |
| D04 | 已完成 | `steps/D04-event-lock-and-batch-compensation.md` |
| D05 | 已完成 | `steps/D05-recovery-message-dispatch.md` |
| D06 | 已完成 | `steps/D06-recovery-callback-state-machine.md` |
| D07 | 已完成 | `steps/D07-recovery-callback-idempotency.md` |
| D08 | 已完成 | `steps/D08-recovery-batch-timeout.md` |
| D09 | 已完成 | `steps/D09-recovery-batch-detail-and-audit.md` |
| D10 | 已完成 | `steps/D10-tm-recovery-regression.md` |

### 阶段 E：Engine 重处理执行

| 步骤 | 状态 | 总结文档 |
| --- | --- | --- |
| E01 | 已完成 | `steps/E01-recovery-message-handler.md` |
| E02 | 已完成 | `steps/E02-dql-recovery-event.md` |
| E03 | 已完成 | `steps/E03-recovery-coordinator.md` |
| E04 | 已完成 | `steps/E04-live-source-read-gate.md` |
| E05 | 已完成 | `steps/E05-recovery-only-runner.md` |
| E06 | 已完成 | `steps/E06-source-boundary-injection.md` |
| E07 | 已完成 | `steps/E07-recovery-barrier.md` |
| E08 | 已完成 | `steps/E08-recovery-capture-guard.md` |
| E09 | 未开始 | 待生成 |
| E10 | 未开始 | 待生成 |

## 回归记录

| 日期 | 范围 | 记录文档 |
| --- | --- | --- |
| 2026-08-27 | A01-A05、B01-B03 最新前端交互与 API 契约回归 | `regressions/2026-08-27-A01-B03-contract-regression.md` |

## 里程碑进度

| 里程碑 | 状态 | 总结文档 |
| --- | --- | --- |
| M0 契约冻结 | 已完成 | `milestones/M0-contract-freeze.md` |
| M1 TM 基础能力 | 已完成 | `milestones/M1-tm-foundation.md` |
| M2 Engine 捕获闭环 | 未开始 | 待生成 |
| M3 TM 批次控制 | 已完成（待集成验证） | `milestones/M3-tm-recovery-batch-control.md` |
| M4 Engine 回放闭环 | 部分完成 | 待生成 |
| M5 告警与初始化 | 未开始 | 待生成 |
| M6 POC 验收 | 未开始 | 待生成 |
