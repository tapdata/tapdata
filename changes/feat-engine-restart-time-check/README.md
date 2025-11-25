# TM启动时间检查功能

## 功能描述

在 `TaskRestartSchedule.engineRestartNeedStartTask()` 方法中添加了TM（Task Manager）自身启动时间检查逻辑。如果TM启动时间小于 10 分钟，则直接返回，不执行后续的任务重启逻辑。

## 修改内容

### 文件位置
- `manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java`

### 修改详情

1. **添加TM启动时间记录**：
```java
// TM启动时间
private static final long TM_START_TIME = System.currentTimeMillis();
```

2. **添加获取TM启动时间的方法**：
```java
/**
 * 获取TM启动时间
 * @return TM启动时间戳
 */
private long getTmStartTime() {
    return TM_START_TIME;
}
```

3. **在方法开始处添加TM启动时间检查**：
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

## 实现逻辑

1. **记录启动时间**：在类加载时通过静态变量记录TM的启动时间
2. **检查运行时间**：在每次执行 `engineRestartNeedStartTask()` 时，计算TM的运行时间
3. **条件判断**：如果TM运行时间小于 10 分钟（600000 毫秒），则直接返回
4. **日志记录**：记录调试日志，说明跳过执行的原因和具体的运行时间

## 测试验证

创建了测试类 `TestLogic.java` 验证逻辑正确性：

### 测试场景
1. **TM启动 5 分钟前**：应该跳过执行 ✓
2. **TM启动 10 分钟前**：应该继续执行 ✓
3. **TM启动 15 分钟前**：应该继续执行 ✓
4. **TM刚启动**：应该跳过执行 ✓

### 测试结果
```
=== 测试TM启动时间检查逻辑 ===
  -> TM运行时间: 300000ms (5分钟)
场景1 - TM启动5分钟前: 跳过执行
场景2 - TM启动10分钟前: 继续执行
场景3 - TM启动15分钟前: 继续执行
  -> TM运行时间: 35ms (0分钟)
场景4 - TM刚启动: 跳过执行
```

所有测试场景都按预期工作，验证了实现的正确性。

## 影响分析

### 正面影响
- **避免误操作**：防止在TM刚启动时就执行任务重启逻辑
- **提高稳定性**：给TM足够的初始化时间，避免在系统未完全就绪时进行任务调度
- **减少不必要的调度**：避免在TM启动初期的频繁任务重启检查

### 注意事项
- 使用静态变量记录启动时间，确保在整个TM生命周期内保持一致
- 使用 debug 级别日志记录，不会影响正常的日志输出
- 只影响 `engineRestartNeedStartTask` 方法，不影响其他调度逻辑

## 配置参数

- **时间阈值**：10 分钟（600000 毫秒）
- **检查频率**：跟随原有的调度频率（每 5 秒执行一次）
- **启动延迟**：原有的 150 秒初始延迟保持不变

## 兼容性

此修改是向后兼容的，不会影响现有功能：
- 不改变任何现有的 API 或配置
- 不影响其他调度任务的执行
- 只在TM启动后的前10分钟内生效，之后恢复正常行为

## 使用场景

这个功能特别适用于以下场景：
- TM重启后需要一段时间来完全初始化
- 避免在系统启动过程中进行不必要的任务重启检查
- 提高系统启动阶段的稳定性
