# 任务启动补偿轮询机制实现

## 需求描述

增加一个轮询机制，查询 agentId 为自身、状态为启动中、且没有被下发到自身队列的任务，启动他们。
轮询间隔：第一次10秒，如果没有任务，逐步提升，每次增加10秒，最大300秒一次。

## 实现方案

### 1. 补偿轮询服务 (TaskStartCompensationSchedule)

#### 核心功能
- **查询条件**：状态为 `SCHEDULING` 且超过30秒没有被处理的任务
- **动态间隔**：10秒起步，无任务时每次增加10秒，最大300秒
- **补偿启动**：使用任务启动队列服务进行补偿

#### 轮询间隔算法
```java
// 基础间隔：10秒
private static final long BASE_INTERVAL_MS = 10 * 1000L;

// 间隔增量：10秒
private static final long INTERVAL_INCREMENT_MS = 10 * 1000L;

// 最大间隔：300秒
private static final long MAX_INTERVAL_MS = 300 * 1000L;

// 当前间隔计算
long interval = BASE_INTERVAL_MS + (multiplier - 1) * INTERVAL_INCREMENT_MS;
return Math.min(interval, MAX_INTERVAL_MS);
```

#### 间隔变化规律
| 轮询次数 | 倍数 | 间隔时间 | 说明 |
|---------|------|----------|------|
| 1 | 1 | 10秒 | 初始间隔 |
| 2 | 2 | 20秒 | 无任务，增加10秒 |
| 3 | 3 | 30秒 | 无任务，再增加10秒 |
| ... | ... | ... | 持续增加 |
| 30 | 30 | 300秒 | 达到最大间隔 |
| 31+ | 30 | 300秒 | 保持最大间隔 |

### 2. 查询逻辑

#### 查询条件
```java
Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING);

// 添加时间条件：schedulingTime超过30秒的任务
long compensationThreshold = System.currentTimeMillis() - 30 * 1000L;
criteria.and("schedulingTime").lt(compensationThreshold);
```

#### 查询字段
- `_id`：任务ID
- `name`：任务名称
- `agentId`：引擎ID
- `schedulingTime`：调度开始时间
- `userId`：用户ID

### 3. 补偿处理流程

```
定时轮询 → 检查轮询间隔
├─ 未到轮询时间 → 跳过
└─ 到达轮询时间 → 检查任务队列状态
   ├─ 队列中有任务 → 跳过补偿（避免重复处理）
   └─ 队列为空 → 查询补偿任务
      ├─ 没有任务 → 增加轮询间隔
      └─ 有任务 → 重置轮询间隔 → 处理补偿任务
         └─ 使用TaskStartQueueService.requestStartTask()
```

### 4. 动态间隔控制

#### 增加间隔逻辑
```java
private void increaseInterval() {
    int currentMultiplier = intervalMultiplier.get();
    long currentInterval = BASE_INTERVAL_MS + (currentMultiplier - 1) * INTERVAL_INCREMENT_MS;
    
    if (currentInterval < MAX_INTERVAL_MS) {
        intervalMultiplier.incrementAndGet();
    }
}
```

#### 重置间隔逻辑
```java
private void resetInterval() {
    intervalMultiplier.set(1); // 重置为基础间隔
}
```

## 实现细节

### 1. 补偿触发条件

#### 队列状态检查
- **队列优先**：如果任务启动队列中有任务，跳过补偿
- **避免重复**：防止补偿机制与正常限流队列冲突
- **代码实现**：
  ```java
  int queueSize = taskStartQueueService.getQueueSize();
  if (queueSize > 0) {
      log.debug("Task start queue has {} pending tasks, skipping compensation", queueSize);
      return; // 跳过补偿
  }
  ```

#### 时间阈值
- **30秒阈值**：任务在 `SCHEDULING` 状态超过30秒
- **避免误判**：给正常处理流程留出足够时间

#### 状态检查
- **精确状态**：只处理 `STATUS_SCHEDULING` 状态的任务
- **避免重复**：不会处理已经在处理中的任务

### 2. 补偿执行方式

#### 使用现有队列服务
```java
boolean startedImmediately = taskStartQueueService.requestStartTask(taskId, agentId, user, "schedule");
```

#### 优势
- **复用限流机制**：自动遵循5秒间隔限流
- **队列保护**：被限流时自动排队等待
- **统一处理**：与正常启动流程一致

### 3. 系统用户处理

#### 用户获取策略
```java
private UserDetail getSystemUser() {
    if (systemUser == null) {
        try {
            systemUser = userService.loadUserByUsername("admin");
            if (systemUser == null) {
                // 创建默认系统用户
                systemUser = createDefaultSystemUser();
            }
        } catch (Exception e) {
            systemUser = createDefaultSystemUser();
        }
    }
    return systemUser;
}
```

## 监控和告警

### 1. 状态监控 (TaskCompensationMonitorSchedule)

#### 监控频率
- **每60秒**输出一次状态信息

#### 监控内容
- 当前轮询间隔
- 间隔倍数
- 上次轮询时间
- 下次轮询剩余时间

#### 监控日志示例
```
Task compensation polling status: interval=30000ms, multiplier=3, queueSize=0, lastPoll=1640995200000, nextPollIn=15000ms
```

### 2. 补偿执行日志

#### 关键日志
- `Task start queue has X pending tasks, skipping compensation` - 队列有任务，跳过补偿
- `Found X tasks need compensation` - 发现需要补偿的任务
- `Compensating task start: taskId=X, waitingTime=Xms` - 开始补偿任务
- `Task compensation executed immediately` - 补偿任务立即执行
- `Task compensation queued due to rate limit` - 补偿任务排队等待

### 3. 手动触发接口

#### 紧急补偿
```java
public void triggerCompensation() {
    log.info("Manually triggering task start compensation");
    lastPollTime.set(0); // 重置时间，强制下次轮询
    resetInterval(); // 重置间隔
}
```

## 性能影响分析

### 1. 数据库查询开销

#### 查询频率
- **最频繁**：每10秒一次
- **最稀疏**：每300秒一次
- **平均频率**：根据系统负载动态调整

#### 查询复杂度
- **简单查询**：只查询状态和时间字段
- **索引友好**：基于status和schedulingTime字段
- **结果集小**：通常只有少量或没有结果

### 2. 内存使用

#### 轮询状态
- **极小开销**：只存储几个原子变量
- **无累积**：不存储历史数据

#### 查询结果
- **临时存储**：查询结果仅在处理期间存储
- **及时释放**：处理完成后立即释放

### 3. 系统负载

#### 正常情况
- **低频轮询**：间隔逐步增加到300秒
- **无额外处理**：没有补偿任务时只做查询

#### 异常情况
- **高频轮询**：有补偿任务时保持10秒间隔
- **限流保护**：补偿任务受现有限流机制保护

## 配置参数

### 可调整参数
```java
// 基础轮询间隔（当前：10秒）
private static final long BASE_INTERVAL_MS = 10 * 1000L;

// 间隔增量（当前：10秒）
private static final long INTERVAL_INCREMENT_MS = 10 * 1000L;

// 最大轮询间隔（当前：300秒）
private static final long MAX_INTERVAL_MS = 300 * 1000L;

// 补偿时间阈值（当前：30秒）
long compensationThreshold = System.currentTimeMillis() - 30 * 1000L;
```

### 调优建议
- **基础间隔**：可根据系统响应要求调整（5-30秒）
- **最大间隔**：可根据容忍度调整（120-600秒）
- **时间阈值**：可根据正常处理时间调整（15-60秒）

## 总结

这个补偿轮询机制提供了强大的任务启动保障：

1. **智能间隔**：根据系统负载动态调整轮询频率
2. **精确补偿**：只处理真正需要补偿的任务
3. **性能友好**：最小化对系统性能的影响
4. **监控完善**：提供详细的状态监控和日志
5. **集成现有机制**：复用限流和队列机制
6. **异常安全**：完善的错误处理和降级策略

特别适合以下场景：
- **消息丢失**：管理端消息未能到达引擎
- **网络问题**：临时网络中断导致的启动失败
- **系统重启**：服务重启期间的任务丢失
- **状态不一致**：各种原因导致的状态不同步

这个机制确保了任务启动的最终一致性，是系统可靠性的重要保障！
