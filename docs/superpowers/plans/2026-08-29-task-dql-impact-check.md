# Task DQL Impact Check Implementation Plan

> **For agentic workers:** Execute this plan task by task. Every implementation task follows TDD: add a failing test, run it and record the red result, implement the smallest behavior, then run focused verification.

## Goal

为 TM 提供任务删除/重置前的 DQL 影响检查接口，并在数据复制列表、数据转换列表及任务详情重置流程中增加非阻塞的 DQL 二次确认。

## Architecture

TM 在 `dql` 域新增 `DqlTaskImpactService`，通过带用户数据权限的任务查询获取当前任务版本，再由 `DqlEventRepository` 使用一次 Mongo 聚合按 `(task_id, task_version)` 统计 `PENDING`、`REPROCESSING`、`RECOVERY_FAILED`。`TaskController` 暴露 `POST /api/Task/dql-event-impact`，不把检查服务接入删除/重置状态机。

Web 在 `packages/api/src/core/task.ts` 增加批量检查封装。业务任务列表集中封装“原确认 → 影响检查 → 影响二次确认 → 原操作”，覆盖数据复制和数据转换共用的 `List.vue` 及其批量入口。任务详情的 reset 调用点复用一个非阻塞确认 helper，覆盖 `MonitorView.vue` 和 `MigrationMonitorSimple.vue`。预检查错误按 fail-open 处理；画布节点删除和空画布清理不接入。

## Task 1: TM request/response and repository counting

**Files:**

- Create `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlTaskImpactRequestVo.java`
- Create `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlTaskImpactVo.java`
- Modify `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- Modify `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`

**Behavior:**

- Empty task-version input returns an empty count map.
- Matching requires task ID, exact task version, and the three non-terminal/reprocessable statuses.
- Add a compound index on task ID, task version and status.

**TDD:**

1. Add a repository test that captures the aggregation and verifies the status list and `(task_id, task_version)` pairs, including two task groups with different versions.
2. Run `mvn -pl manager/tm -Dtest=DqlEventRepositoryTest test`; expect failure because the method/index do not exist.
3. Implement the repository aggregation and update the index-count assertions.
4. Rerun the focused repository test and `git diff --check`.

## Task 2: TM impact service and controller

**Files:**

- Create `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlTaskImpactService.java`
- Create `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlTaskImpactServiceTest.java`
- Create `manager/tm/src/test/java/com/tapdata/tm/task/controller/TaskControllerTest.java`
- Modify `manager/tm/src/main/java/com/tapdata/tm/task/controller/TaskController.java`

**Behavior:**

- Return one result per distinct valid input ID in request order.
- Query only non-deleted tasks visible to the current user, projecting `_id` and `version`.
- Return `exists=false,count=0` for missing, deleted, invalid or invisible IDs.
- Do not initialize missing task versions or mutate task/DQL data.
- Controller returns `ResponseMessage<List<DqlTaskImpactVo>>` and supports both `/api/Task` and `/api/task` class aliases.

**TDD:**

1. Add service tests for empty input, missing task, null version, multi-task versions/counts and input ordering; add controller mapping/delegation test.
2. Run focused tests; expect compilation/test failure because service, endpoint and setter are absent.
3. Implement service and controller with the existing `@Setter(onMethod_ = @Autowired)` injection style.
4. Run service/controller tests and `mvn -pl manager/tm -Dtest=DqlEventRepositoryTest,DqlTaskImpactServiceTest,TaskControllerTest test`.

## Task 3: Web API and shared list confirmation

**Files:**

- Modify `packages/api/src/core/task.ts`
- Modify `packages/api/src/Task.ts` for the legacy wrapper consistency
- Modify `packages/business/src/views/task/List.vue`
- Modify `packages/business/src/locale/lang/zh-CN.js`
- Modify `packages/business/src/locale/lang/zh-TW.js`
- Modify `packages/business/src/locale/lang/en.js`

**Behavior:**

- Add typed `checkTaskDqlImpact(taskIds)` API wrapper using the batch request body.
- Keep the existing standard confirmation unchanged.
- After standard confirmation, call the impact API.
- For affected tasks, render task names and individual counts in a second confirmation with delete/reset-specific copy.
- On impact API failure or malformed response, continue the original operation.
- On second-confirm cancellation, do not call the original mutation.
- Use the selected-row map for bulk names and the clicked row for single operations.

**TDD/static checks:**

1. Add or extend the repository’s available frontend test coverage if present; otherwise add testable pure helpers for impact filtering/message entries and validate them with the existing package test tooling.
2. Run the focused frontend test/type check before implementation and capture the expected missing API/helper failure.
3. Implement the API wrappers, async confirmation helper and i18n strings.
4. Run `pnpm exec vue-tsc --noEmit` (or the repository’s available type-check command) and `pnpm check-i18n`.

## Task 4: Web detail reset confirmation

**Files:**

- Modify `packages/dag/src/composables/useCanvasOperation.ts`
- Modify `packages/dag/src/MigrationMonitorSimple.vue`
- Modify `packages/dag/src/locale/lang/zh-CN.js`
- Modify `packages/dag/src/locale/lang/zh-TW.js`
- Modify `packages/dag/src/locale/lang/en.js`

**Behavior:**

- After the existing detail reset confirmation, call the shared task impact API.
- If matching records exist, show reset-specific DQL warning and proceed only after confirmation.
- If the check fails, continue reset as before.
- Keep node deletion and page-return cleanup untouched.

**TDD/static checks:**

1. Add focused helper/component tests if the package has a runnable test harness; otherwise verify call ordering through type-safe extracted helper logic and static call-site inspection.
2. Run the focused check before implementation, then implement the smallest helper reuse.
3. Run type-check/lint for the affected packages and inspect all reset call sites with `rg`.

## Task 5: Integration verification and documentation

**Files:**

- Update `docs/superpowers/specs/2026-08-29-task-dql-impact-check-design.md` only if implementation reveals a contract change.
- Add a short progress note under `doc/TAP-12615-DQL-development-progress/steps/` if the project progress convention requires it.

**Verification:**

- TM focused unit tests and TM module compile/test.
- Web type-check and i18n validation; package lint/type check if available.
- `git diff --check` in both repositories.
- Final `rg` audit confirms list delete/reset and detail reset call the impact helper, while node deletion does not.
- Review both repository diffs for unrelated changes before reporting completion.

