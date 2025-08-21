# 动态任务超时机制实现

## 需求描述

在管理端，300秒超时的逻辑需要调整：如果有100个启动中的任务，则任务的接管超时应该为3000秒。

## 问题分析

### 原有超时机制的问题：
1. **固定超时时间**：无论有多少任务在排队，都是固定的300秒超时
2. **不考虑队列长度**：没有考虑启动中任务的数量对接管时间的影响
3. **容易误判超时**：当有大量任务排队时，300秒可能不够

### 实际场景：
- **少量任务**：300秒足够完成接管
- **大量任务**：由于限流机制（5秒一个），100个任务需要500秒才能全部启动
- **队列积压**：任务越多，后面的任务等待时间越长

## 解决方案

### 动态超时计算公式

```java
if (schedulingTaskCount >= 100) {
    // 100个或以上任务，使用3000秒
    dynamicHeartExpire = 3000000L; // 3000秒
} else if (schedulingTaskCount > 0) {
    // 根据任务数量线性调整：300秒 + (任务数量 / 100) * 2700秒
    double multiplier = 1.0 + (schedulingTaskCount / 100.0) * 9.0; // 1倍到10倍之间
    dynamicHeartExpire = (long) (baseHeartExpire * multiplier);
} else {
    // 没有启动中的任务，使用基础超时时间
    dynamicHeartExpire = baseHeartExpire;
}
```

### 超时时间对照表

| 启动中任务数量 | 超时时间 | 倍数 | 说明 |
|---------------|----------|------|------|
| 0个 | 300秒 | 1倍 | 基础超时时间 |
| 10个 | 570秒 | 1.9倍 | 小量任务，适度延长 |
| 25个 | 975秒 | 3.25倍 | 中等任务量 |
| 50个 | 1650秒 | 5.5倍 | 较多任务 |
| 75个 | 2325秒 | 7.75倍 | 大量任务 |
| 100个+ | 3000秒 | 10倍 | 最大超时时间 |

### 计算逻辑说明

1. **基础超时时间**：从配置中读取，默认300秒
2. **任务数量查询**：实时查询状态为 `SCHEDULING` 的任务数量
3. **线性调整**：根据任务数量在1倍到10倍之间线性调整
4. **上限保护**：最大不超过3000秒

## 实现细节

### 1. 新增方法：getDynamicHeartExpire()

```java
private long getDynamicHeartExpire() {
    // 获取基础超时时间
    long baseHeartExpire = getHeartExpire();
    
    // 查询当前启动中的任务数量
    Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING);
    long schedulingTaskCount = taskService.count(Query.query(criteria));
    
    // 动态调整超时时间
    long dynamicHeartExpire;
    if (schedulingTaskCount >= 100) {
        dynamicHeartExpire = 3000000L; // 3000秒
    } else if (schedulingTaskCount > 0) {
        double multiplier = 1.0 + (schedulingTaskCount / 100.0) * 9.0;
        dynamicHeartExpire = (long) (baseHeartExpire * multiplier);
    } else {
        dynamicHeartExpire = baseHeartExpire;
    }
    
    return dynamicHeartExpire;
}
```

### 2. 修改超时检查逻辑

```java
// 原来：使用固定超时时间
long heartExpire = getHeartExpire();

// 修改后：使用动态超时时间
long heartExpire = getDynamicHeartExpire();
```

### 3. 增强日志记录

```java
// 添加详细的超时日志
log.warn("Task scheduling timeout detected: taskId={}, agentId={}, schedulingElapsed={}ms, dynamicTimeout={}ms", 
        taskDto.getId().toHexString(), taskDto.getAgentId(), schedulingElapsed, heartExpire);

// 更新错误消息
String template = "The engine[{0}] takes over the task with a timeout of {1}ms (dynamic timeout based on scheduling queue).";
```

## 预期效果

### 1. 减少误判超时
- **少量任务**：仍然使用300秒，快速检测真正的超时
- **大量任务**：自动延长超时时间，避免正常排队被误判为超时

### 2. 智能适应负载
- **自动调整**：根据实际队列长度动态调整
- **线性增长**：超时时间随任务数量平滑增长
- **上限保护**：最大3000秒，避免无限等待

### 3. 更好的可观测性
- **详细日志**：记录任务数量、超时时间等关键信息
- **动态监控**：可以观察超时时间的动态变化
- **问题诊断**：更容易判断是真正超时还是正常排队

## 监控建议

### 关键日志
- `Dynamic heart expire calculated: schedulingTaskCount=x, baseHeartExpire=xms, dynamicHeartExpire=xms`
- `Task scheduling timeout detected: taskId=x, agentId=x, schedulingElapsed=xms, dynamicTimeout=xms`

### 监控指标
1. **启动中任务数量**：观察队列长度变化
2. **动态超时时间**：观察超时时间的调整
3. **实际超时频率**：是否有效减少误判
4. **任务启动成功率**：整体成功率是否提升

## 配置建议

如果需要调整动态超时的参数，可以修改以下值：

1. **最大倍数**：当前是10倍，可以根据实际情况调整
2. **线性系数**：当前是每100个任务增加9倍，可以调整斜率
3. **最大超时时间**：当前是3000秒，可以根据业务需求调整

## 总结

这个动态超时机制能够：

1. **智能适应**：根据实际队列长度自动调整超时时间
2. **减少误判**：避免正常排队的任务被误判为超时
3. **保持保护**：仍然能够检测真正的超时情况
4. **提升稳定性**：减少不必要的任务重启和状态变更

特别是在有100个任务排队的场景下，超时时间会自动调整到3000秒，给任务足够的时间完成接管过程。
