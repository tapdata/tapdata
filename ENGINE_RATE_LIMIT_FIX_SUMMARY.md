# 引擎端限流循环排队问题修复

## 问题分析

从日志可以看出存在**循环排队**问题：

```
[INFO] TaskStartRateLimitService - Task start from queue: taskId=685bcaf8e5f1f434cc876cf4, ...
[INFO] TapdataTaskScheduler - Starting pending task: ...
[INFO] TapdataTaskScheduler - Send start task operation: ...
[INFO] TaskStartRateLimitService - Task start queued: taskId=685bcaf8e5f1f434cc876cf4, queueSize=298, remainingMs=9999
```

**问题根源：**
1. `processPendingTasks()` 从队列取出任务
2. 调用 `sendStartTask()` 发送启动操作
3. `Start-Task-Operation-Handler` 处理时调用 `startTask()`
4. `startTask()` 再次检查限流，发现刚记录了启动时间，又把任务放回队列
5. 形成无限循环

## 修复方案

### 1. 修改任务启动方法签名
```java
// 原来
protected void startTask(TaskDto taskDto, boolean isRetry)

// 修改后
protected void startTask(TaskDto taskDto, boolean isRetry, boolean skipRateLimit)
```

### 2. 添加专门的排队任务启动方法
```java
/**
 * 启动排队任务，跳过限流检查
 */
protected void startQueuedTask(TaskDto taskDto) {
    startTask(taskDto, false, true);
}
```

### 3. 修改processPendingTasks逻辑
```java
// 原来：调用sendStartTask()导致重复排队
sendStartTask(nextTask);

// 修改后：直接启动，跳过限流检查
if (!taskLock.tryRun(taskId, () -> startQueuedTask(nextTask), 1L, TimeUnit.SECONDS)) {
    // 如果获取锁失败，重新放回队列
    taskStartRateLimitService.requeueTask(nextTask);
}
```

### 4. 分离启动时间记录逻辑
```java
// getNextPendingTask()不再记录启动时间
public TaskDto getNextPendingTask() {
    // 只取出任务，不记录启动时间
    logger.info("Task dequeued for start: ...");
    return taskDto;
}

// 在实际启动时记录
if (skipRateLimit) {
    taskStartRateLimitService.recordQueuedTaskStart(taskId, taskDto.getName());
}
```

## 修复后的流程

### 正常任务启动流程
1. 接收启动请求 → `startTask(taskDto, false, false)`
2. 检查限流 → `requestStartTask(taskDto, false)`
3. 如果限流 → 加入队列
4. 如果不限流 → 立即启动并记录时间

### 排队任务启动流程
1. 定时检查 → `processPendingTasks()`
2. 取出任务 → `getNextPendingTask()`（不记录启动时间）
3. 直接启动 → `startQueuedTask(nextTask)`（跳过限流检查）
4. 记录启动时间 → `recordQueuedTaskStart()`

### 重试任务启动流程
1. 任务重试 → `startRetryTask(taskDto)`
2. 直接启动 → `startTask(taskDto, true, false)`
3. 重试任务不受限流影响，立即启动

## 关键改进

1. **消除循环排队**：排队任务启动时跳过限流检查
2. **保持限流效果**：新任务仍然受10秒间隔限制
3. **重试任务优先**：重试任务不受限流影响
4. **锁失败处理**：获取任务锁失败时重新排队，避免任务丢失

## 预期效果

修复后应该看到：
- 队列中的任务能够正常启动，不再循环排队
- 新任务仍然受限流控制，10秒启动一个
- 重试任务立即启动，不受限流影响
- 任务不会丢失，都能最终启动
