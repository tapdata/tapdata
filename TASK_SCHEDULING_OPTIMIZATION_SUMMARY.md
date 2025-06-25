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
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TaskStartRateLimitService.java`

#### 修改的文件：
- `manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java`
- `iengine/iengine-common/src/main/java/com/tapdata/constant/ConnectorConstant.java`

#### 限流规则：
- **操作间隔限制**：同一任务的操作（启动/停止/重置/调度）间隔必须大于10秒
- **重试冷却期**：首次下发完成后10分钟内不进行重试
- **支持的操作类型**：start、stop、reset、schedule

#### 实现机制：
- 使用 `ConcurrentHashMap` 存储任务的最后操作时间和首次下发完成时间
- 在每次操作前检查是否满足限流条件
- 操作成功后记录操作时间和首次下发完成时间
- 定期清理过期记录（24小时）避免内存泄漏
- **关键改进**：在任务调度（接管）阶段也进行限流控制，确保10秒内只会接管一个任务
- **引擎端限流**：在引擎端任务启动流程中也添加限流控制，确保10秒内只有一个任务会进入启动流程

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

### TaskStartRateLimitService（引擎端）

**主要方法：**
- `requestStartTask(taskDto, isRetry)`: 请求启动任务，重试任务立即启动，普通任务检查限流
- `getNextPendingTask()`: 获取下一个等待启动的任务
- `getPendingTaskCount()`: 获取等待队列大小
- `getRemainingTime()`: 获取距离下次可启动的剩余时间
- `reset()`: 重置限流状态和清空队列

**配置参数：**
- `START_INTERVAL_MS`: 任务启动间隔时间（10秒）

**特点：**
- **智能区分**：重试任务不受限流影响，立即启动
- **任务排队**：使用BlockingQueue存储等待启动的任务
- **自动处理**：定时任务每秒检查并启动下一个可用任务
- **线程安全**：支持多线程并发调用

## 影响分析

### 正面影响：
1. **减少系统负载**：定时检查频率降低，减少数据库查询和系统资源消耗
2. **避免频繁操作**：限流机制防止任务操作过于频繁，保护系统稳定性
3. **智能重试**：避免无效重试，减少资源浪费
4. **内存管理**：定期清理过期记录，防止内存泄漏

### 可能的影响：
1. **响应延迟**：定时检查频率降低可能导致问题发现和处理的延迟增加（最多120秒）
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
3. 确保任务调度（接管）操作也遵循10秒间隔限制

**修改位置：**
- `TaskServiceImpl.start()`: 用户主动启动时的调度限流
- `TaskRestartSchedule.engineRestartNeedStartTask()`: 引擎重启后任务恢复的调度限流
- `TaskRestartSchedule.schedulingTask()`: 调度重试的限流
- `TaskRestartSchedule.waitRunTask()`: 等待运行任务重新调度的限流

### 引擎端任务启动限流

**问题描述：**
虽然Manager端已经有了调度限流，但引擎端在接收到任务启动消息后，仍然会立即启动任务，没有额外的限流保护。

**关键问题修复：**
1. **任务卡在启动中**：之前的限流机制导致大量任务卡在启动中状态
2. **重试任务被限流**：任务重试不应该受限流影响，但之前的实现会阻止重试
3. **任务丢失**：触发限流时任务被直接丢弃，没有排队机制

**解决方案：**
1. 创建 `TaskStartRateLimitService` 引擎端限流服务
2. 在 `TapdataTaskScheduler.startTask()` 方法中添加启动限流检查
3. **重试任务不受限流影响**：`isRetry=true` 的任务立即启动
4. **任务排队机制**：触发限流时任务进入队列，定时处理
5. 保持多线程接收任务的逻辑不变，只在启动时进行限流

**实现特点：**
- **全局限流**：整个引擎实例10秒内只能启动一个任务
- **保持并发接收**：多线程接收任务消息的逻辑保持不变
- **启动时限流**：只在实际启动任务时进行限流控制
- **重试任务优先**：任务重试不受限流影响，立即启动
- **任务排队**：触发限流时任务不会丢失，而是排队等待启动
- **自动处理**：定时检查排队任务，自动启动下一个可用任务

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
