# 引擎启动任务频率控制配置说明

## 配置参数

### 1. startTask 全局限流间隔

**位置**：`TapdataTaskScheduler` 类成员变量

```java
// 当前配置：每5秒最多执行一次 startTask
private static final long START_TASK_RATE_LIMIT_MILLIS = 5000L; // 5秒限流间隔
```

**参数说明**：
- `START_TASK_RATE_LIMIT_MILLIS`: 5000 - 限流间隔时间（毫秒）
- 适用于所有调用 `startTask` 方法的场景
- 使用同步锁确保线程安全

### 2. 引擎启动任务频率控制间隔

**位置**：`TapdataTaskScheduler.startEngineStartTaskScheduler()`

```java
// 当前配置：每10秒处理一个任务
engineStartTaskScheduler.scheduleWithFixedDelay(this::processEngineStartTaskQueue, 0, 10, TimeUnit.SECONDS);
```

**参数说明**：
- `initialDelay`: 0 - 立即开始执行
- `delay`: 10 - 间隔时间（秒）
- `unit`: TimeUnit.SECONDS - 时间单位

### 3. 调度器线程配置

**位置**：`TapdataTaskScheduler` 类成员变量

```java
private final ScheduledExecutorService engineStartTaskScheduler = 
    new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Engine-Start-Task-Scheduler"));
```

**参数说明**：
- `corePoolSize`: 1 - 核心线程数
- `threadFactory`: 自定义线程工厂，设置线程名称

### 4. 队列配置

**位置**：`TapdataTaskScheduler` 类成员变量

```java
private final LinkedBlockingQueue<TaskDto> engineStartTaskQueue = new LinkedBlockingQueue<>();
```

**特点**：
- 无界队列，理论上可以存储无限数量的任务
- 线程安全，支持并发操作

### 5. 资源清理超时

**位置**：`TapdataTaskScheduler.destroy()`

```java
if (!engineStartTaskScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
    logger.warn("Engine start task scheduler did not terminate gracefully, forcing shutdown");
    engineStartTaskScheduler.shutdownNow();
}
```

**参数说明**：
- `timeout`: 30 - 等待时间（秒）
- `unit`: TimeUnit.SECONDS - 时间单位

## 可调整的配置项

### 1. 修改 startTask 全局限流间隔

如需调整 startTask 方法的限流频率，修改以下代码：

```java
// 例如：改为3秒间隔
private static final long START_TASK_RATE_LIMIT_MILLIS = 3000L;

// 例如：改为10秒间隔
private static final long START_TASK_RATE_LIMIT_MILLIS = 10000L;
```

### 2. 修改引擎启动任务频率间隔

如需调整任务启动频率，修改以下代码：

```java
// 例如：改为5秒间隔
engineStartTaskScheduler.scheduleWithFixedDelay(this::processEngineStartTaskQueue, 0, 5, TimeUnit.SECONDS);

// 例如：改为30秒间隔
engineStartTaskScheduler.scheduleWithFixedDelay(this::processEngineStartTaskQueue, 0, 30, TimeUnit.SECONDS);
```

### 3. 修改线程池大小

如需并发处理多个任务，可以增加线程池大小：

```java
// 例如：使用2个线程并发处理
private final ScheduledExecutorService engineStartTaskScheduler = 
    new ScheduledThreadPoolExecutor(2, r -> new Thread(r, "Engine-Start-Task-Scheduler"));
```

**注意**：增加线程数会降低频率控制的效果。

### 4. 修改队列类型

如需限制队列大小，可以使用有界队列：

```java
// 例如：最多存储100个任务
private final LinkedBlockingQueue<TaskDto> engineStartTaskQueue = new LinkedBlockingQueue<>(100);
```

### 5. 修改资源清理超时

如需调整关闭等待时间：

```java
// 例如：等待60秒
if (!engineStartTaskScheduler.awaitTermination(60, TimeUnit.SECONDS)) {
    // ...
}
```

## 性能调优建议

### 1. 频率间隔选择

| 间隔时间 | 适用场景 | 优点 | 缺点 |
|---------|---------|------|------|
| 5秒 | 任务数量少，需要快速启动 | 启动速度快 | 系统负载较高 |
| 10秒 | 一般场景，平衡性能和速度 | 平衡性能 | 中等启动速度 |
| 30秒 | 任务数量多，系统资源紧张 | 系统负载低 | 启动速度慢 |

### 2. 线程池大小

- **单线程**：严格的频率控制，任务按顺序启动
- **多线程**：提高并发度，但会降低频率控制效果

### 3. 队列大小

- **无界队列**：不会丢失任务，但可能占用大量内存
- **有界队列**：限制内存使用，但可能导致任务丢失

## 监控指标

### 1. 队列大小监控

```java
// 获取当前队列大小
int queueSize = engineStartTaskQueue.size();
logger.info("Engine start task queue size: {}", queueSize);
```

### 2. 处理速度监控

```java
// 记录处理开始时间
long startTime = System.currentTimeMillis();

// 处理任务
sendStartTask(task);

// 记录处理耗时
long duration = System.currentTimeMillis() - startTime;
logger.info("Task startup duration: {}ms", duration);
```

### 3. 调度器状态监控

```java
// 检查调度器状态
boolean isShutdown = engineStartTaskScheduler.isShutdown();
boolean isTerminated = engineStartTaskScheduler.isTerminated();
logger.info("Scheduler status - shutdown: {}, terminated: {}", isShutdown, isTerminated);
```

## 故障排查

### 1. 任务启动缓慢

**可能原因**：
- 频率间隔设置过大
- 任务启动本身耗时较长
- 系统资源不足

**解决方案**：
- 减少频率间隔
- 优化任务启动逻辑
- 增加系统资源

### 2. 内存占用过高

**可能原因**：
- 队列中积压大量任务
- 任务对象占用内存较大

**解决方案**：
- 使用有界队列
- 优化任务对象结构
- 增加处理频率

### 3. 调度器无法关闭

**可能原因**：
- 任务处理时间过长
- 线程被阻塞

**解决方案**：
- 增加关闭超时时间
- 优化任务处理逻辑
- 使用强制关闭
