# 任务调度优化总结

## 修改概述

本次优化主要针对任务调度系统进行了四个方面的调整：

1. **定时检查频率调整**：将所有定时检查逻辑的频率统一调整为120秒
2. **任务操作限流**：新增任务操作限流机制，防止频繁操作导致资源过载
3. **重试策略优化**：优化下发重试逻辑，避免粗暴重试
4. **引擎心跳频率调整**：将引擎端所有心跳上报频率调整为30秒

## 详细修改内容

### 1. 定时检查频率调整（120秒）

#### 修改的文件：
- `manager/tm/src/main/java/com/tapdata/tm/schedule/AgentUpdateSchedule.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/MigrateDagSchedule.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`
- `tm-enterprise/src/main/java/com/tapdata/tm/taskinspect/schedule/TaskInspectSchedule.java`

#### 修改内容：
- **AgentUpdateSchedule**: 从每10秒调整为每120秒
- **MigrateDagSchedule**: 
  - `migrateDagPlanStart`: 从每10秒调整为每120秒
  - `startPlanCronTask`: 从每10秒调整为每120秒
- **TaskRestartSchedule**:
  - `restartTask`: 从每60秒调整为每120秒
  - `engineRestartNeedStartTask`: 从每5秒调整为每120秒
  - `overTimeTask`: 从每30秒调整为每120秒
- **TaskInspectSchedule**: 从每10秒调整为每120秒

### 2. 任务操作限流机制

#### 新增文件：
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskOperationRateLimitService.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskOperationRateLimitCleanupSchedule.java`
- `manager/tm/src/main/java/com/tapdata/tm/base/exception/TaskScheduleRateLimitException.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/EngineTaskStartRateLimitService.java`
- `ENGINE_BYPASS_RATE_LIMIT_FIX.md`

#### 修改的文件：
- `manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java`

#### 限流规则：
- **操作间隔限制**：同一引擎的操作（启动/停止/重置/调度）间隔必须大于5秒
- **重试冷却期**：首次下发完成后10分钟内不进行重试
- **支持的操作类型**：start、stop、reset、schedule
- **按引擎限流**：每个引擎独立限流，不同引擎之间不相互影响

#### 实现机制：
- 使用 `ConcurrentHashMap` 存储引擎的最后操作时间和任务的首次下发完成时间
- 在每次操作前检查是否满足限流条件
- 操作成功后记录操作时间和首次下发完成时间
- 定期清理过期记录（24小时）避免内存泄漏
- **关键改进**：在任务调度（接管）阶段也进行限流控制，确保5秒内只会接管一个任务
- **按引擎限流**：管理端改为按引擎分别限流，每个引擎独立5秒间隔，不同引擎之间不相互影响
- **仅管理端限流**：移除引擎端限流，只在管理端进行统一的限流控制
- **修复绕过限流**：修复引擎端定时调度和启动恢复绕过限流的问题，添加引擎端10秒限流机制

### 按引擎限流机制详解

**原来的限流方式：**
- Key: `taskId + ":" + operationType`
- 问题：所有引擎共享限流，一个引擎的操作会影响其他引擎

**新的限流方式：**
- Key: `agentId + ":" + operationType`
- 优势：每个引擎独立限流，互不影响

**示例：**
```
引擎A: agent-001:start -> 可以5秒启动一个任务
引擎B: agent-002:start -> 可以5秒启动一个任务
引擎C: agent-003:start -> 可以5秒启动一个任务
```

**API变更：**
```java
// 原来
canExecuteOperation(taskId, operationType)
recordOperation(taskId, operationType)

// 现在
canExecuteOperation(taskId, agentId, operationType)
recordOperation(taskId, agentId, operationType)
```

### 3. 重试策略优化

#### 修改的文件：
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`

#### 优化内容：
- **停止任务重试**：在重试前检查冷却期，避免频繁重试
- **调度任务重试**：在重新调度前检查冷却期
- **等待运行任务重试**：在重新调度前检查冷却期

## 核心类说明

### TaskOperationRateLimitService

**主要方法：**
- `canExecuteOperation(taskId, operationType)`: 检查是否可以执行操作
- `recordOperation(taskId, operationType)`: 记录操作执行
- `canRetryOperation(taskId)`: 检查是否可以重试
- `recordFirstDeliveryComplete(taskId)`: 记录首次下发完成时间
- `cleanupExpiredRecords()`: 清理过期记录

**配置参数：**
- `OPERATION_INTERVAL_MS`: 操作间隔时间（10秒）
- `RETRY_COOLDOWN_MS`: 重试冷却时间（10分钟）

### TaskOperationRateLimitCleanupSchedule

**功能：**
- 每小时执行一次过期记录清理
- 清理超过24小时的限流记录
- 防止内存泄漏



## 影响分析

### 正面影响：
1. **减少系统负载**：限流机制减少频繁操作，降低系统资源消耗
2. **避免频繁操作**：限流机制防止任务操作过于频繁，保护系统稳定性
3. **智能重试**：避免无效重试，减少资源浪费
4. **内存管理**：定期清理过期记录，防止内存泄漏
5. **按引擎限流**：每个引擎独立限流，提高多引擎环境下的并行处理能力

### 可能的影响：
1. **操作延迟**：限流机制可能导致任务操作的延迟（最多5秒）
2. **操作限制**：限流机制可能在某些紧急情况下限制必要的操作

### 建议监控指标：
1. 任务状态异常的发现时间
2. 任务操作的成功率和延迟
3. 系统资源使用情况（CPU、内存、数据库连接）
4. 限流触发的频率和原因

## 关键问题解决

### 任务接管限流问题

**问题描述：**
用户批量启动任务时，虽然消息发送有限流，但任务的**接管（调度）**过程没有限流控制，导致任务接管速度远超10秒一条的预期。

**根本原因：**
- 任务启动流程：用户点击 → 状态机处理 → **任务调度（接管）** → 发送消息
- 之前的限流只控制了"发送消息"环节，没有控制"任务调度"环节
- 批量启动时，多个任务会并发进行调度，绕过了消息发送的限流

**解决方案：**
1. 在 `TaskServiceImpl.start()` 方法中的调度调用前添加限流检查
2. 在 `TaskRestartSchedule` 的所有调度重试场景中添加限流检查
3. 确保任务调度（接管）操作也遵循5秒间隔限制
4. 改为按引擎分别限流，每个引擎独立5秒间隔

**修改位置：**
- `TaskServiceImpl.start()`: 用户主动启动时的调度限流
- `TaskRestartSchedule.engineRestartNeedStartTask()`: 引擎重启后任务恢复的调度限流
- `TaskRestartSchedule.schedulingTask()`: 调度重试的限流
- `TaskRestartSchedule.waitRunTask()`: 等待运行任务重新调度的限流



## 配置建议

如果需要调整限流参数，可以修改 `TaskOperationRateLimitService` 中的常量：
- 调整操作间隔时间：修改 `OPERATION_INTERVAL_MS`
- 调整重试冷却时间：修改 `RETRY_COOLDOWN_MS`
- 调整记录过期时间：修改 `cleanupExpiredRecords()` 方法中的过期时间

## 部署注意事项

1. 确保所有相关的Spring Bean都正确注入
2. 验证定时任务的执行频率是否符合预期
3. 监控限流机制是否正常工作
4. 观察系统性能是否有改善
5. 关注任务调度的响应时间是否在可接受范围内
