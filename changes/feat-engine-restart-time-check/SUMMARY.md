# TM启动时间检查功能实现总结

## 需求描述
在 `engineRestartNeedStartTask` 方法中添加TM自身启动时间检查，如果进程启动时间小于 10 分钟，就直接 return。

## 实现方案

### 核心思路
- 使用静态变量记录TM启动时间
- 在每次执行 `engineRestartNeedStartTask()` 时检查TM运行时间
- 如果运行时间小于10分钟，直接返回，跳过后续逻辑

### 代码修改

#### 1. 添加TM启动时间记录
```java
// TM启动时间
private static final long TM_START_TIME = System.currentTimeMillis();
```

#### 2. 添加获取方法
```java
/**
 * 获取TM启动时间
 * @return TM启动时间戳
 */
private long getTmStartTime() {
    return TM_START_TIME;
}
```

#### 3. 添加启动时间检查逻辑
```java
// 检查TM自身启动时间，如果小于10分钟则直接返回
long currentTime = System.currentTimeMillis();
long tmRunningTime = currentTime - getTmStartTime();
if (tmRunningTime < 600000L) { // 10分钟 = 600000毫秒
    log.debug("TM started less than 10 minutes ago, skipping engineRestartNeedStartTask. TM running time: {} ms ({} minutes)", 
        tmRunningTime, tmRunningTime / 1000 / 60);
    return;
}
```

## 测试验证

### 测试用例
1. **TM启动5分钟前** → 跳过执行 ✓
2. **TM启动10分钟前** → 继续执行 ✓  
3. **TM启动15分钟前** → 继续执行 ✓
4. **TM刚启动** → 跳过执行 ✓

### 测试结果
所有测试用例都通过，验证了实现的正确性。

## 技术特点

### 优点
- **简单高效**：使用静态变量，无额外性能开销
- **精确可靠**：基于系统时间戳，精度高
- **向后兼容**：不影响现有功能
- **日志友好**：提供详细的调试信息

### 设计考虑
- **静态变量**：确保在整个TM生命周期内启动时间保持一致
- **毫秒精度**：使用 `System.currentTimeMillis()` 提供足够的时间精度
- **调试日志**：使用 debug 级别，不影响正常日志输出
- **常量定义**：10分钟阈值（600000毫秒）清晰明确

## 影响分析

### 正面影响
- 避免TM启动初期的误操作
- 提高系统启动阶段的稳定性
- 减少不必要的任务重启检查

### 风险控制
- 只影响 `engineRestartNeedStartTask` 方法
- 不改变任何现有API或配置
- 10分钟后自动恢复正常行为

## 部署建议

### 验证步骤
1. 重启TM服务
2. 观察前10分钟内的日志，应该看到跳过执行的debug日志
3. 10分钟后确认 `engineRestartNeedStartTask` 正常执行

### 监控要点
- 关注TM启动后前10分钟的行为
- 确认10分钟后任务重启检查功能正常工作
- 观察系统整体稳定性是否有改善

## 文件清单

- **主要修改**：`manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`
- **测试文件**：`changes/feat-engine-restart-time-check/TestLogic.java`
- **单元测试**：`changes/feat-engine-restart-time-check/TaskRestartScheduleTest.java`
- **说明文档**：`changes/feat-engine-restart-time-check/README.md`

## 总结

本次修改成功实现了TM启动时间检查功能，通过简单而有效的方式避免了TM启动初期的任务重启检查，提高了系统的稳定性。实现方案经过充分测试验证，具有良好的兼容性和可维护性。
