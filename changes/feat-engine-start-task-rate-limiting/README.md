# 引擎启动任务接管频率控制优化

## 概述

对 `TapdataTaskScheduler.runTaskIfNeedWhenEngineStart()` 方法进行了优化，增加了频率控制机制，避免引擎启动时同时启动大量任务造成系统负载过高。

## 修改内容

### 1. 新增成员变量

在 `TapdataTaskScheduler` 类中新增了以下成员变量：

```java
// 引擎启动时任务接管的频率控制
private final LinkedBlockingQueue<TaskDto> engineStartTaskQueue = new LinkedBlockingQueue<>();
private final ScheduledExecutorService engineStartTaskScheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Engine-Start-Task-Scheduler"));
private volatile boolean engineStartTaskSchedulerStarted = false;

// startTask 方法的限流控制
private final Object startTaskLock = new Object();
private volatile long lastStartTaskTime = 0L;
private static final long START_TASK_RATE_LIMIT_MILLIS = 5000L; // 5秒限流间隔
```

### 2. 修改 runTaskIfNeedWhenEngineStart 方法

**原逻辑**：
- 查找属于当前引擎的运行中任务
- 立即启动所有找到的任务

**新逻辑**：
- 查找属于当前引擎的运行中任务
- 将任务加入队列
- 启动调度器，每10秒处理一个任务

### 3. startTask 方法限流增强

**原逻辑**：
- 直接执行任务启动逻辑

**新逻辑**：
- 在方法开始时应用限流控制
- 确保任何调用源都不能超过5秒一个的频率

### 4. 新增方法

#### applyStartTaskRateLimit()
- 应用 startTask 方法的限流控制
- 使用同步锁确保线程安全
- 计算等待时间并强制等待
- 更新最后执行时间

#### startEngineStartTaskScheduler()
- 启动引擎启动任务调度器
- 使用同步锁确保只启动一次
- 设置10秒的固定延迟调度

#### processEngineStartTaskQueue()
- 处理引擎启动任务队列
- 每次从队列中取出一个任务进行启动
- 包含异常处理和日志记录

#### destroy() (@PreDestroy)
- 资源清理方法
- 优雅关闭调度器
- 清空任务队列

## 技术特点

### 1. 双重频率控制
- **引擎启动任务频率**：每10秒启动一个任务
- **startTask 全局限流**：不管什么调用源，每5秒最多执行一次
- **队列**：使用 `LinkedBlockingQueue` 存储待启动任务
- **调度器**：使用 `ScheduledExecutorService` 实现定时处理

### 2. 线程安全
- 使用 `volatile` 关键字确保调度器启动状态的可见性
- 使用 `synchronized` 方法确保调度器只启动一次
- 使用线程安全的队列存储任务

### 3. 资源管理
- 添加 `@PreDestroy` 方法确保资源正确释放
- 优雅关闭调度器，等待30秒后强制关闭
- 清空队列中的剩余任务

### 4. 日志记录
- 详细记录任务入队过程
- 记录任务启动过程
- 记录异常和错误信息
- 记录资源清理过程

## 使用场景

### 适用场景
- 云版本引擎启动时的任务恢复
- 大量任务需要重启的场景
- 需要控制系统负载的环境

### 生效条件
- 仅在云版本 (`AppType.currentType().isCloud()`) 中生效
- 引擎启动时自动触发
- 存在状态为 `running` 且 `agentId` 匹配的任务

## 性能影响

### 优势
- **降低系统负载**：避免同时启动大量任务
- **提高稳定性**：减少资源竞争和冲突
- **可控的启动速度**：10秒间隔提供可预测的启动节奏

### 考虑因素
- **启动时间延长**：大量任务的完全启动时间会延长
- **内存占用**：队列会暂时存储待启动的任务对象
- **线程资源**：新增一个调度器线程

## 配置说明

当前频率控制参数硬编码为10秒，如需调整可修改以下位置：

```java
// 在 startEngineStartTaskScheduler() 方法中
engineStartTaskScheduler.scheduleWithFixedDelay(this::processEngineStartTaskQueue, 0, 10, TimeUnit.SECONDS);
```

## 兼容性

- **向后兼容**：不影响现有功能
- **云版本专用**：仅在云版本中生效
- **优雅降级**：如果调度器启动失败，不会影响其他功能

## 测试建议

1. **功能测试**：验证任务是否按10秒间隔启动
2. **负载测试**：测试大量任务场景下的性能表现
3. **异常测试**：测试调度器异常情况下的处理
4. **资源测试**：验证资源清理是否正确执行
