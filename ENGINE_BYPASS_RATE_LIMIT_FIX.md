# 修复引擎端绕过限流机制的问题

## 问题描述

用户发现引擎启动时会直接查找自己应该运行的任务去运行，这个逻辑绕过了管理端的限流机制。

## 问题分析

经过代码分析，发现确实存在两个地方会绕过管理端的限流：

### 1. `scheduledTask()` 方法
- **调用频率**：每1秒执行一次
- **功能**：查找状态为 `STATUS_WAIT_RUN` 且 `agentId` 匹配的任务
- **问题**：直接调用 `sendStartTask(taskDto)` 启动任务，绕过管理端限流

### 2. `runTaskIfNeedWhenEngineStart()` 方法  
- **调用时机**：引擎启动时执行一次
- **功能**：查找状态为 `STATUS_RUNNING` 且 `agentId` 匹配的任务
- **问题**：直接调用 `sendStartTask(taskDto)` 启动任务，绕过管理端限流

## 问题影响

1. **限流失效**：引擎可以同时启动多个任务，完全绕过5秒间隔限制
2. **批量启动**：引擎启动时可能同时启动大量任务，造成系统负载过高
3. **不一致性**：用户手动启动受限流控制，但引擎自动启动不受控制

## 修复方案

### 修改策略
在引擎端添加简单的限流机制，保持直接启动任务的方式，但确保10秒内只启动一个任务。

### 具体修改

#### 1. 新增 `EngineTaskStartRateLimitService`
- 简单的限流服务，使用 `AtomicLong` 记录最后启动时间
- 10秒间隔限制
- 线程安全的实现

#### 2. 修改 `scheduledTask()` 方法
```java
// 原来：直接启动所有任务，无限流控制
for (TaskDto waitRunTask : allWaitRunTasks) {
    sendStartTask(taskDto);
}

// 修改后：持续尝试启动，确保所有任务最终都能启动
while (true) {
    List<TaskDto> allWaitRunTasks = clientMongoOperator.find(query, ...);
    if (allWaitRunTasks.isEmpty()) break; // 没有任务需要启动

    boolean taskStarted = false;
    for (TaskDto waitRunTask : allWaitRunTasks) {
        if (!engineTaskStartRateLimitService.canStartTask(taskId, taskName)) {
            continue; // 跳过被限流的任务
        }
        engineTaskStartRateLimitService.recordTaskStart(taskId, taskName);
        sendStartTask(taskDto);
        taskStarted = true;
        break; // 一次只启动一个任务
    }

    if (!taskStarted) {
        Thread.sleep(10000); // 等待10秒后重试
    }
}
```

#### 3. 修改 `runTaskIfNeedWhenEngineStart()` 方法
```java
// 原来：直接启动所有任务，无限流控制
tasks.forEach(this::sendStartTask);

// 修改后：持续尝试启动，确保所有任务最终都能启动
while (true) {
    List<TaskDto> tasks = clientMongoOperator.find(query, ...);
    if (CollectionUtils.isEmpty(tasks)) break; // 没有任务需要启动

    boolean taskStarted = false;
    for (TaskDto task : tasks) {
        if (!engineTaskStartRateLimitService.canStartTask(taskId, taskName)) {
            continue; // 跳过被限流的任务
        }
        engineTaskStartRateLimitService.recordTaskStart(taskId, taskName);
        sendStartTask(task);
        taskStarted = true;
        break; // 一次只启动一个任务
    }

    if (!taskStarted) {
        Thread.sleep(10000); // 等待10秒后重试
    }
}
```

## 修复效果

### 修复前的问题场景：
1. **引擎启动时**：可能同时启动10+个任务，无限流控制
2. **定时调度时**：每秒可能启动多个等待任务，无限流控制
3. **用户手动启动**：受5秒限流控制

### 修复后的效果：
1. **引擎启动时**：直接启动任务，但受10秒限流控制，一次只启动一个
2. **定时调度时**：直接启动任务，但受10秒限流控制，一次只启动一个
3. **用户手动启动**：受管理端5秒限流控制
4. **双重保护**：引擎端10秒限流 + 管理端5秒限流

## 流程对比

### 修复前的流程：
```
引擎定时调度 → 直接查询数据库 → 直接启动任务 (绕过限流)
引擎启动恢复 → 直接查询数据库 → 直接启动任务 (绕过限流)
用户手动启动 → 管理端API → 限流检查 → 发送启动消息 → 引擎启动任务
```

### 修复后的流程：
```
引擎定时调度 → 直接查询数据库 → 引擎端限流检查 → 直接启动任务 (10秒间隔)
引擎启动恢复 → 直接查询数据库 → 引擎端限流检查 → 直接启动任务 (10秒间隔)
用户手动启动 → 管理端API → 管理端限流检查 → 发送启动消息 → 引擎启动任务 (5秒间隔)
```

## 注意事项

### 1. 异常处理
- 如果管理端API调用失败，会记录警告日志但不会中断流程
- 避免因网络问题导致任务无法启动

### 2. 性能考虑
- 直接启动任务，无网络开销
- 引擎端限流简单高效，性能开销极小

### 3. 兼容性
- 修改不影响现有的任务启动逻辑
- 只是改变了启动请求的路径，从直接启动改为API请求

## 验证方法

### 1. 引擎启动验证
1. 停止引擎
2. 在数据库中设置多个任务状态为 `STATUS_RUNNING`
3. 启动引擎
4. 观察任务启动是否遵循5秒间隔

### 2. 定时调度验证  
1. 在数据库中设置多个任务状态为 `STATUS_WAIT_RUN`
2. 观察引擎定时调度是否遵循5秒间隔启动任务

### 3. 日志验证
查看日志中是否出现：
- `Engine task start rate limited`
- `Engine task start recorded`
- `Engine restart task with rate limit`

## 总结

这个修复在引擎端添加了简单有效的限流机制，确保引擎端直接启动任务时也遵循10秒间隔限制，消除了绕过限流的漏洞。同时保持了直接启动的高性能，避免了网络开销，提高了系统的稳定性。

### 关键优势：
1. **性能优化**：直接启动任务，无网络开销
2. **简单有效**：使用AtomicLong实现，线程安全且高效
3. **双重保护**：引擎端10秒限流 + 管理端5秒限流
4. **一次一个**：确保每次只启动一个任务，避免批量启动
5. **确保完整性**：持续重试直到所有任务都启动，不会遗漏任务
6. **智能等待**：被限流时等待10秒后重试，避免无效循环
