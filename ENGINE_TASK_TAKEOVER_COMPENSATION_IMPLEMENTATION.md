# 引擎端任务接管补偿机制实现

## 需求描述

增加补偿机制，由引擎发起，查询状态是启动中、自身 agentId 接管的任务，进行接管。

补偿机制条件：
1. 自己启动 10分钟后，且自身已经超过 1分钟没有接管新的任务时，开始补偿
2. 补偿初始间隔 10秒，如果没有需要补偿的任务，调整为 10分钟一次

## 实现方案

### 1. 补偿服务 (TaskTakeoverCompensationService)

#### 核心功能
- **查询条件**：状态为 `SCHEDULING`（启动中），agentId 为当前引擎，且超过2分钟没有被处理
- **补偿条件**：引擎启动10分钟后 + 最后接管任务1分钟后
- **动态间隔**：10秒起步，无任务时调整为10分钟

#### 补偿触发条件
```java
// 1. 引擎启动时间检查
if (currentTime - engineStartTime.get() < ENGINE_STARTUP_WAIT_MS) {
    return false; // 引擎启动不足10分钟
}

// 2. 最后接管时间检查
if (lastTakeover > 0 && currentTime - lastTakeover < LAST_TAKEOVER_WAIT_MS) {
    return false; // 最后接管任务不足1分钟
}

// 3. 补偿间隔检查
if (lastCompensation > 0 && currentTime - lastCompensation < interval) {
    return false; // 补偿间隔未到
}
```

#### 补偿间隔算法
| 场景 | 间隔时间 | 说明 |
|------|----------|------|
| 有补偿任务 | 10秒 | 快速处理积压任务 |
| 无补偿任务 | 10分钟 | 降低系统负载 |

### 2. 查询逻辑

#### 查询条件
```java
Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING)
        .and("agentId").is(instanceNo);

// 添加时间条件：schedulingTime超过2分钟的任务
long compensationThreshold = System.currentTimeMillis() - 2 * 60 * 1000L;
criteria.and("schedulingTime").lt(compensationThreshold);
```

#### 查询字段
- `_id`：任务ID
- `name`：任务名称
- `agentId`：引擎ID
- `schedulingTime`：调度开始时间

### 3. 补偿处理流程

```
定时检查(每10秒) → 检查补偿条件
├─ 引擎启动不足10分钟 → 跳过
├─ 最后接管不足1分钟 → 跳过
├─ 补偿间隔未到 → 跳过
└─ 满足补偿条件 → 查询补偿任务
   ├─ 没有任务 → 调整间隔为10分钟
   └─ 有任务 → 重置间隔为10秒 → 处理补偿任务
      └─ 调用taskScheduler.sendStartTask()
```

### 4. 接管时间记录

#### 记录时机
```java
// 在任务启动成功后记录接管时间
if (taskDto != null) {
    engineTaskStartRateLimitService.recordTaskStart(taskId, taskName);
    // 记录任务接管时间
    if (taskTakeoverCompensationService != null) {
        taskTakeoverCompensationService.recordTaskTakeover();
    }
    sendStartTask(taskDto);
}
```

#### 记录位置
- **scheduledTask()**: 正常任务调度时
- **runTaskIfNeedWhenEngineStart()**: 引擎启动恢复时

## 实现细节

### 1. 补偿触发时机

#### 引擎启动等待期
- **等待时间**：10分钟
- **目的**：确保引擎完全启动并稳定运行
- **避免误判**：防止引擎启动期间的正常延迟被误认为需要补偿

#### 最后接管等待期
- **等待时间**：1分钟
- **目的**：确保引擎确实没有在处理新任务
- **避免冲突**：防止与正常任务接管流程冲突

### 2. 补偿查询条件

#### 时间阈值
- **2分钟阈值**：任务在 `SCHEDULING` 状态超过2分钟
- **避免误判**：给正常处理流程留出足够时间
- **比管理端更宽松**：管理端是30秒，引擎端是2分钟

#### 状态检查
- **精确状态**：只处理 `STATUS_SCHEDULING` 状态的任务
- **引擎匹配**：只处理分配给当前引擎的任务

### 3. 补偿执行方式

#### 使用现有机制
```java
taskScheduler.sendStartTask(task);
```

#### 优势
- **复用限流机制**：自动遵循引擎端5秒间隔限流
- **统一处理**：与正常启动流程一致
- **避免冲突**：不会绕过现有的保护机制

### 4. 一次处理一个任务

#### 处理策略
```java
// 只处理一个任务，避免一次性处理太多
break;
```

#### 原因
- **避免过载**：防止一次性启动大量任务
- **遵循限流**：与5秒间隔限流机制配合
- **逐步处理**：确保每个任务都能得到充分处理

## 集成方式

### 1. TapdataTaskScheduler 集成

#### 服务初始化
```java
private void initTaskTakeoverCompensationService() {
    if (taskTakeoverCompensationService == null) {
        taskTakeoverCompensationService = new TaskTakeoverCompensationService(
                mongoTemplate, this, instanceNo);
    }
}
```

#### 定时调度
```java
// 每10秒检查一次补偿条件
taskResetRetryServiceScheduledThreadPool.scheduleWithFixedDelay(
    this::performTaskTakeoverCompensation, 10L, 10L, TimeUnit.SECONDS);
```

### 2. 接管时间记录

#### 正常任务调度
- 在 `scheduledTask()` 方法中记录
- 任务启动成功后立即记录

#### 引擎启动恢复
- 在 `runTaskIfNeedWhenEngineStart()` 方法中记录
- 任务恢复成功后立即记录

## 监控和告警

### 1. 补偿状态监控

#### 状态信息
```java
public String getCompensationStatus() {
    return String.format("TaskTakeoverCompensation[%s]: uptime=%dms, lastTakeover=%dms, lastCompensation=%dms, interval=%dms", 
            instanceNo, engineUptime, lastTakeover, lastCompensation, currentInterval);
}
```

#### 监控内容
- 引擎运行时间
- 最后接管时间
- 最后补偿时间
- 当前补偿间隔

### 2. 补偿执行日志

#### 关键日志
- `Starting task takeover compensation for engine: X` - 开始补偿
- `Found X tasks need compensation, reset interval to: 10000ms` - 发现补偿任务
- `No tasks need compensation, adjusted interval to: 600000ms` - 无任务，调整间隔
- `Compensating task takeover: taskId=X, waitingTime=Xms` - 开始补偿任务
- `Task compensation takeover completed: taskId=X` - 补偿完成

### 3. 手动触发接口

#### 紧急补偿
```java
public void triggerCompensation() {
    log.info("Manually triggering task takeover compensation for: {}", instanceNo);
    lastCompensationTime.set(0); // 重置时间，强制下次补偿
    currentCompensationInterval.set(INITIAL_COMPENSATION_INTERVAL_MS); // 重置间隔
}
```

## 性能影响分析

### 1. 数据库查询开销

#### 查询频率
- **正常情况**：每10分钟一次（无任务时）
- **异常情况**：每10秒一次（有任务时）
- **启动期间**：不执行查询（10分钟等待期）

#### 查询复杂度
- **简单查询**：只查询状态、agentId和时间字段
- **索引友好**：基于status、agentId和schedulingTime字段
- **结果集小**：通常只有少量或没有结果

### 2. 内存使用

#### 补偿状态
- **极小开销**：只存储几个原子变量
- **无累积**：不存储历史数据

#### 查询结果
- **临时存储**：查询结果仅在处理期间存储
- **及时释放**：处理完成后立即释放

### 3. 系统负载

#### 正常情况
- **低频查询**：间隔逐步增加到10分钟
- **无额外处理**：没有补偿任务时只做查询

#### 异常情况
- **高频查询**：有补偿任务时保持10秒间隔
- **限流保护**：补偿任务受现有限流机制保护

## 配置参数

### 可调整参数
```java
// 引擎启动等待时间（当前：10分钟）
private static final long ENGINE_STARTUP_WAIT_MS = 10 * 60 * 1000L;

// 最后接管等待时间（当前：1分钟）
private static final long LAST_TAKEOVER_WAIT_MS = 1 * 60 * 1000L;

// 初始补偿间隔（当前：10秒）
private static final long INITIAL_COMPENSATION_INTERVAL_MS = 10 * 1000L;

// 无任务补偿间隔（当前：10分钟）
private static final long NO_TASK_COMPENSATION_INTERVAL_MS = 10 * 60 * 1000L;

// 补偿时间阈值（当前：2分钟）
long compensationThreshold = System.currentTimeMillis() - 2 * 60 * 1000L;
```

### 调优建议
- **启动等待时间**：可根据引擎启动速度调整（5-15分钟）
- **接管等待时间**：可根据任务处理频率调整（30秒-5分钟）
- **补偿间隔**：可根据系统负载调整（5秒-30秒 / 5分钟-30分钟）
- **时间阈值**：可根据正常处理时间调整（1-5分钟）

## 总结

这个引擎端补偿机制提供了强大的任务接管保障：

1. **智能触发**：只在真正需要时执行补偿
2. **避免冲突**：与现有机制完美配合
3. **性能友好**：最小化对系统性能的影响
4. **监控完善**：提供详细的状态监控和日志
5. **集成现有机制**：复用限流和启动流程
6. **异常安全**：完善的错误处理和降级策略

特别适合以下场景：
- **消息丢失**：管理端消息未能到达引擎
- **网络问题**：临时网络中断导致的启动失败
- **状态不一致**：各种原因导致的状态不同步
- **引擎重启**：引擎重启后的任务恢复

这个补偿机制是引擎端任务接管可靠性的重要保障，确保任务接管的最终一致性！
