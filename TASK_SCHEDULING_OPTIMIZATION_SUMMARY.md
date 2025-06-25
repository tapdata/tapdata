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

#### 修改的文件：
- `manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskServiceImpl.java`

#### 限流规则：
- **操作间隔限制**：同一任务的操作（启动/停止/重置）间隔必须大于10秒
- **重试冷却期**：首次下发完成后10分钟内不进行重试
- **支持的操作类型**：start、stop、reset

#### 实现机制：
- 使用 `ConcurrentHashMap` 存储任务的最后操作时间和首次下发完成时间
- 在每次操作前检查是否满足限流条件
- 操作成功后记录操作时间和首次下发完成时间
- 定期清理过期记录（24小时）避免内存泄漏

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
