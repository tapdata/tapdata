# 任务接管间隔调整：从10秒改为5秒，按引擎分别限流

## 修改概述

本次更新主要包含两个方面的调整：
1. **间隔时间调整**：将任务接管间隔从10秒调整为5秒
2. **限流机制优化**：管理端改为按引擎分别限流，而不是所有引擎共享限流
3. **架构简化**：移除引擎端限流，只在管理端进行统一限流控制

## 1. 管理端修改

### 修改文件：
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskOperationRateLimitService.java`
- `manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/task/service/TaskServiceImpl.java`
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`

### 核心变更：

#### 2.1 间隔时间调整
```java
// 原来：10秒间隔
private static final long OPERATION_INTERVAL_MS = 10 * 1000L;

// 修改后：5秒间隔
private static final long OPERATION_INTERVAL_MS = 5 * 1000L;
```

#### 2.2 限流机制改为按引擎分别限流
```java
// 原来：按任务限流
private final ConcurrentHashMap<String, AtomicLong> lastOperationTimes; // Key: taskId

// 修改后：按引擎限流
private final ConcurrentHashMap<String, AtomicLong> lastOperationTimes; // Key: agentId:operationType
```

#### 2.3 API方法签名变更
```java
// 原来
public boolean canExecuteOperation(String taskId, String operationType)
public void recordOperation(String taskId, String operationType)

// 修改后
public boolean canExecuteOperation(String taskId, String agentId, String operationType)
public void recordOperation(String taskId, String agentId, String operationType)
```

#### 2.4 所有调用点更新
所有调用TaskOperationRateLimitService的地方都更新为传入agentId：
- `sendStartMsg()`: 启动消息发送
- `sendStoppingMsg()`: 停止消息发送
- `renew()`: 重置操作
- `start()`: 任务启动调度
- `TaskRestartSchedule`: 各种重试场景

## 2. 架构简化

### 移除的文件：
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TaskStartRateLimitService.java`

### 回退的修改：
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java`
- `iengine/iengine-common/src/main/java/com/tapdata/constant/ConnectorConstant.java`

### 简化原因：
1. **避免双重限流**：管理端和引擎端同时限流可能导致过度限制
2. **统一控制**：在管理端统一进行限流控制，更容易管理和监控
3. **减少复杂性**：简化架构，减少维护成本

## 3. 效果对比

### 修改前：
- **间隔时间**：10秒
- **限流范围**：所有引擎共享限流
- **问题**：多个引擎时，总体接管速度受限

### 修改后：
- **间隔时间**：5秒
- **限流范围**：每个引擎独立限流
- **优势**：
  - 单个引擎：5秒接管一个任务（比原来快1倍）
  - 多个引擎：每个引擎都可以5秒接管一个任务（总体速度大幅提升）

## 4. 实际效果示例

### 场景：3个引擎，每个引擎有10个任务需要启动

#### 修改前（10秒全局限流）：
- 总时间：10秒 × 30个任务 = 300秒（5分钟）
- 每个引擎平均：100秒完成10个任务

#### 修改后（5秒按引擎限流）：
- 总时间：5秒 × 10个任务 = 50秒（每个引擎并行）
- 每个引擎：50秒完成10个任务
- **性能提升：6倍**

## 5. 兼容性处理

为了保持向后兼容，保留了原有的API方法：
```java
// 兼容方法（会输出警告日志）
public boolean canExecuteOperation(String taskId, String operationType)
public void recordOperation(String taskId, String operationType)
```

## 6. 监控建议

建议监控以下指标：
1. **每个引擎的任务启动频率**：应该接近5秒一个
2. **引擎间的负载均衡**：各引擎的任务分配是否均匀
3. **总体任务启动速度**：相比修改前应该有显著提升
4. **限流触发频率**：每个引擎独立统计

## 7. 配置调整建议

如果需要进一步调整间隔时间，可以修改以下常量：
- 管理端：`TaskOperationRateLimitService.OPERATION_INTERVAL_MS`

由于只在管理端进行限流，配置更加简单统一。
