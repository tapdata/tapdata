# 📘 Tapdata 节点开发手册

**基于 HazelcastMergeNode（主从合并节点）完整实现的权威指南**

> **📖 适用场景：** 在 Tapdata UI 的数据转换模块画布上开发自定义数据处理节点
>
> **🎯 参考模板：** HazelcastMergeNode（主从合并节点）- 1400+ 行生产级代码
>
> **📚 前置知识：** Java、React/JavaScript、Formily Schema、Hazelcast 分布式缓存

---

## 📌 手册结构导航

1. [第一章：节点架构总览 - 三层体系与数据流](#第一章节点架构总览---三层体系与数据流)
2. [第二章：后端配置类开发 - MergeTableNode.java](#第二章后端配置类开发---mergetablenodejava)
3. [第三章：前端 UI 组件开发 - MergeTable.js](#第三章前端-ui-组件开发---mergetablejs)
4. [第四章：引擎处理器开发 - HazelcastMergeNode.java](#第四章引擎处理器开发---hazelcastmergenodejava)
5. [第五章：节点注册与集成机制](#第五章节点注册与集成机制)
6. [第六章：测试验证与最佳实践](#第六章测试验证与最佳实践)
7. [第七章：完整开发检查清单](#第七章完整开发检查清单)

---

# 第一章：节点架构总览 - 三层体系与数据流

## 1.1 三层架构模型

### 🎨 第一层：前端 UI

**文件位置：**
```
tapdata-web/packages/dag/
  src/nodes/MergeTable.js
```

**职责：**
- 定义节点类型标识
- 配置表单 Schema
- 实现用户交互逻辑
- 参数校验与联动

**技术栈：**
- Formily (表单框架)
- React 组件
- i18n 国际化

> 用户可见的配置界面

### ⚙️ 第二层：配置类

**文件位置：**
```
tapdata/manager/tm-common/
  src/main/java/com/tapdata/tm/
    commons/dag/process/
      MergeTableNode.java
```

**职责：**
- 定义配置属性字段
- 序列化/反序列化控制
- Schema 合并逻辑
- DAG 图遍历支持

**注解体系：**
- `@NodeType("type_id")`
- `@EqField` (可比较字段)
- `@Getter/@Setter`

> 前后端数据桥梁

### 🔧 第三层：引擎处理器

**文件位置：**
```
tapdata/iengine/iengine-app/
  src/main/java/io/tapdata/flow/
    engine/V2/node/hazelcast/
      processor/
        HazelcastMergeNode.java
```

**职责：**
- 数据接收与处理
- 业务逻辑实现
- 状态管理与缓存
- 事件发射到下游

**运行环境：**
- Hazelcast 分布式引擎
- 多线程并发处理
- 内存/磁盘混合缓存

> 核心业务执行单元

## 1.2 数据流转全生命周期

```
👤 Step 1 → 💾 Step 2 → 🔄 Step 3 → ⚙️ Step 4 → 📤 Step 5
用户配置    前端序列化   后端反序列化  引擎初始化    输出下游
节点        JSON         对象          & 处理       节点
```

**💡 关键说明：**

- 每一层通过 **DAG（有向无环图）** 节点 ID 关联
- 配置数据以 **JSON 格式** 在前后端传输
- 引擎层通过 **Hazelcast IMap** 实现分布式状态共享
- 数据事件封装为 **TapdataEvent** 对象流转

---

# 第二章：后端配置类开发 - MergeTableNode.java

## 2.1 文件结构与基础模板

**📍 文件路径：**

```
tapdata/manager/tm-common/src/main/java/com/tapdata/tm/commons/dag/process/MergeTableNode.java
```

**📦 类继承关系：**

```
ProcessorNode (抽象基类)
  └─ MergeTableNode (具体实现)
       ├─ @NodeType("merge_table_processor")  // 节点类型标识
       ├─ @Getter @Setter                      // Lombok 注解
       └─ @Slf4j                               // 日志支持
```

## 2.2 核心代码示例 - 完整骨架

> ✅ **源码参考：** MergeTableNode.java 第 1-80 行

```java
package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.ProcessorNode;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import java.util.*;

/**
 * 多表合并原地更新模式调回产品（主从合并）
 *
 * @author samuel
 * @create 2022-03-23
 */
@NodeType("merge_table_processor")  // ⭐ 必须注解：节点类型标识（唯一）
@Getter
@Setter
@Slf4j
public class MergeTableNode extends ProcessorNode {

    // ========== 常量定义 ==========
    public static final String MAIN_TABLE_FIRST_MERGE_MODE = "main_table_first";
    public static final String SUB_TABLE_FIRST_MERGE_MODE = "sub_table_first";

    // ========== 配置属性（带 @EqField 注解）==========
    @EqField  // ⭐ 标记为可比较字段（用于 DAG 变更检测）
    private List<MergeTableProperties> mergeProperties;

    @EqField
    private String mergeMode = MAIN_TABLE_FIRST_MERGE_MODE;

    // ========== 构造函数 ==========
    public MergeTableNode() {
        super("merge_table_processor");  // ⭐ 必须调用父构造器，传入节点类型
    }

    // ========== Schema 合并方法（可选但推荐）==========
    @Override
    protected Schema loadSchema(List<String> includes) {
        return null;  // 处理器节点通常返回 null
    }

    /**
     * 把多个属性里面的表，根据关联条件合并成一个宽表
     * 这是节点的核心业务逻辑之一：定义如何合并输入 Schema
     */
    @Override
    public Schema mergeSchema(List<Schema> inputSchemas,
                               Schema schema,
                               DAG.Options options) {
        // TODO: 实现 Schema 合并逻辑
        // 将多个输入表的字段合并为宽表结构

        return super.mergeSchema(Lists.newArrayList(main), null, options);
    }
}
```

## 2.3 关键注解详解

| 注解 | 作用 | 使用场景 | 示例 |
|------|------|----------|------|
| `@NodeType("id")` | 注册节点类型 | 每个节点必须且唯一 | `"duckdb_sql"` |
| `@EqField` | 标记可比较字段 | 用于 DAG 变更检测和任务重启判断 | 所有用户配置的属性 |
| `@JsonProperty` | JSON 序列化名称映射 | 前端字段名 ≠ Java 字段名时使用 | `@JsonProperty("sqlQuery")` |
| `@JsonAlias` | 反序列化别名 | 兼容旧版本字段名 | `@JsonAlias({"querySql"})` |

---

# 第三章：前端 UI 组件开发 - MergeTable.js

## 3.1 Formily Schema 规范速查表

| 属性前缀 | 作用域 | 常用值 | 说明 |
|----------|--------|--------|------|
| `x-` 开头 | Formily 扩展 | component, decorator, visible, reactions | 所有 UI 相关配置 |
| `x-component` | 组件类型 | Input, Select, Switch, ArrayTable, Radio.Group | 决定渲染什么组件 |
| `x-decorator` | 装饰器 | FormItem, Card, Block | 包装组件，提供布局、标签、校验 |
| `x-visible` | 条件显示 | `'{{$form.values.xxx}}'` | 动态显隐控制 |
| `x-reactions` | 响应式逻辑 | 异步数据加载、联动、副作用 | 复杂交互逻辑 |

## 3.2 标准组件库参考

| 组件名 | 用途 | 使用场景 |
|--------|------|----------|
| `Input` | 文本输入框 | 名称、SQL、路径等 |
| `InputNumber` | 数字输入框 | 批大小、超时时间等数值参数 |
| `Select` | 下拉选择 | 枚举选项（存储类型、模式等） |
| `Radio.Group` | 单选按钮组 | 二选一场景（合并模式） |
| `Switch` | 开关切换 | 启用/禁用功能（DuckLake、物化视图） |
| `ArrayTable` | 动态表格 | 列表项增删（关联条件、从表列表） |
| `FormTab` | Tab 容器 | 分组展示（基础/高级/DuckLake/物化视图） |

---

# 第四章：引擎处理器开发 - HazelcastMergeNode.java

## 4.1 核心生命周期方法

### 🔄 阶段 1：初始化 (doInit)

```java
@Override
protected void doInit(@NotNull Context context) throws TapCodeException {
    super.doInit(context);                              // 1. 调用父类初始化

    nodeLogger = ObsLoggerFactory.getInstance()...       // 2. 初始化日志

    if (AppType.currentType().isCloud())
        externalStorageDto = ExternalStorageUtil...      // 3. 云端外存配置

    initRuntimeParameters();                             // 4. ⭐ 加载所有运行时参数

    this.createIndexEvent = new TapdataEvent();         // 5. 创建索引事件

    initLookUpThreadPool();                              // 6. 初始化查询线程池
    initHandleUpdateJoinKeyThreadPool();                 // 7. 初始化更新处理线程池

    PDKIntegration.registerMemoryFetcher(...);          // 8. 注册内存监控
}
```

> **⏰ 调用时机：** 节点启动时仅调用一次 | **🎯 核心任务：** 读取配置、初始化资源、准备运行环境

### 🔄 阶段 2：运行时参数初始化 (initRuntimeParameters)

```java
protected void initRuntimeParameters() {
    autoMarkIsArrayByParentModel();                     // 自动标记数组类型
    selfCheckNode(getNode());                           // 节点自检
    initFirstLevelIds();                                // 初始化第一级节点 ID
    initMergeTableProperties();                         // ⭐ 加载合并配置
    initArrayJoinTargetMap();                           // 初始化数组关联目标
    initLookupMergeProperties();                        // 初始化查找属性
    initMergeCache();                                   // ⭐ 初始化 Hazelcast 缓存
    initSourceNodeMap(null);                            // 初始化源节点映射
    initSourceConnectionMap(null);                      // 初始化连接信息
    initSourcePkOrUniqueFieldMap(null);                 // 初始化主键映射
    initSourceNodeLevelMap(null, 1);                    // 初始化层级映射
    initShareJoinKeys();                                // 初始化共享关联键
    initMergeTablePropertyReferenceMap();               // 初始化属性引用
    initCheckJoinKeyUpdateCacheMap();                   // 初始化更新缓存
}
```

> **⏰ 调用时机：** doInit 内 + updateNodeConfig 时 | **🎯 核心任务：** 将配置类转换为运行时可用的数据结构

### 🔄 阶段 3：批量事件处理 (tryProcess)

```java
@Override
protected void tryProcess(List<BatchEventWrapper> tapdataEvents,
                    Consumer<List<BatchProcessResult>> consumer) {
    StopWatch stopWatch = new StopWatch();

    try {
        stopWatch.start();

        // 1. 发送索引创建事件（首次）
        if (this.createIndexEvent != null) {
            consumer.accept(...);
            this.createIndexEvent = null;
        }

        // 2. 遍历事件，分类处理
        for (BatchEventWrapper wrapper : tapdataEvents) {
            if (controlOrIgnoreEvent(wrapper)) continue;

            if (needCache(wrapper)) batchCache.add(wrapper);

            wrapMergeInfo(wrapper);                       // ⭐ 包装合并信息
        }

        // 3. 批量写入缓存
        if (!batchCache.isEmpty()) doBatchCache(batchCache);

        // 4. 并发查询（多线程）
        doBatchLookUpConcurrent(tapdataEvents, lookupCfs);

        // 5. 输出结果到下游
        for (...) {
            consumer.accept(tapdataEvent, ProcessResult.create());
        }

    } finally {
        stopWatch.stop();
        // 清理资源，帮助 GC
        lookupCfs = null;
        batchCache = null;
    }
}
```

> **⏰ 调用时机：** 每收到一批事件时调用（高频） | **🎯 核心任务：** 接收事件 → 缓存 → 查询 → 合并 → 输出

---

# 第五章：节点注册与集成机制

## 5.1 节点类型标识一致性规则

⚠️ **黄金法则**：三层的节点类型标识必须完全一致！

| 层次 | 定义位置 | 示例值 | 错误后果 |
|------|----------|--------|----------|
| **前端 JS** | `type = 'xxx'` | `'merge_table_processor'` | ❌ **运行时异常**<br>节点无法识别<br>配置无法加载<br>任务直接失败 |
| **后端 Java** | `@NodeType("xxx")` | `@NodeType("merge_table_processor")` | |
| **构造函数** | `super("xxx")` | `super("merge_table_processor")` | |

## 5.2 节点注册流程

1. **前端注册**：在 `packages/dag/src/nodes/index.js` 中导出新节点类
2. **后端扫描**：Spring 自动扫描带 `@NodeType` 注解的类
3. **DAG 构建**：用户保存任务时，前后端通过 type 匹配建立关联
4. **引擎实例化**：任务启动时，引擎根据 type 反射创建对应的 Processor 实例
5. **配置注入**：将用户配置的 JSON 反序列化为 Java 对象，传入 Processor

---

# 第六章：测试验证与最佳实践

## 6.1 测试分层策略

### 单元测试
- ✅ 配置类序列化/反序列化
- ✅ Schema 合并逻辑
- ✅ 参数验证规则
- ✅ 边界条件处理

**工具：** JUnit 5 + Mockito

### 集成测试
- ✅ 前后端配置传输
- ✅ 引擎初始化流程
- ✅ 事件处理管道
- ✅ 缓存读写一致性

**工具：** Spring Boot Test + Testcontainers

### E2E 测试
- ✅ 完整任务编排
- ✅ 多节点协同工作
- ✅ 故障恢复机制
- ✅ 性能压力测试

**工具：** Selenium/Playwright + API 测试

## 6.2 最佳实践清单

1. **遵循现有模式**：优先参考同类型节点（如 MergeTableNode）的实现方式
2. **保持向后兼容**：使用 `@JsonAlias` 兼容旧版本字段名
3. **合理使用注解**：`@EqField` 仅标记需要比较的用户配置字段
4. **优雅降级**：核心功能失败时提供 fallback 方案
5. **完善日志**：关键步骤记录 DEBUG/INFO 级别日志
6. **性能优化**：批量操作优于单条操作，异步优于同步
7. **资源管理**：在 finally 块中清理资源，防止内存泄漏
8. **i18n 支持**：所有用户可见文本使用国际化 key

---

# 第七章：完整开发检查清单

## ✅ 发布前必检项目

### 📝 后端配置类

- [ ] `@NodeType` 注解正确且唯一
- [ ] 构造函数调用 `super(type)`
- [ ] 所有配置属性标注 `@EqField`
- [ ] 复杂对象实现 `toString()`
- [ ] 提供 `mergeSchema()` 实现

### 🔧 引擎处理器

- [ ] 正确继承基类（如 HazelcastProcessorBaseNode）
- [ ] 实现 `doInit()` 初始化方法
- [ ] 实现 `tryProcess()` 事件处理方法
- [ ] 异常处理完善（try-catch-finally）
- [ ] 日志输出规范（使用 nodeLogger）
- [ ] 资源释放完整（线程池、缓存）

### 🎨 前端 UI 组件

- [ ] `type` 与后端完全一致
- [ ] `formSchema` 结构符合 Formily 规范
- [ ] 所有 title 使用 `i18n.t()`
- [ ] 必填字段设置 `required: true`
- [ ] 条件显示使用 `x-visible`
- [ ] 自定义组件 Props 类型安全

### 🧪 测试与文档

- [ ] 单元测试覆盖率 > 80%
- [ ] 集成测试通过 CI 流水线
- [ ] 性能测试满足 SLA 要求
- [ ] 用户手册已编写完成
- [ ] API 文档已更新
- [ ] Changelog 已记录变更

---

## 🎉 恭喜！您已完成 Tapdata 节点开发学习

现在您已经掌握了基于 HazelcastMergeNode 的完整开发流程

**下一步：** 选择一个实际场景开始您的第一个自定义节点开发！

### 📚 相关资源

- **HazelcastMergeNode.java** (1400行)
- **源码位置：** `iengine/iengine-app/src/main/java/.../processor/`

---

**文档生成日期：** 2026-05-29
**适用版本：** Tapdata Platform
**参考源码：** HazelcastMergeNode.java (1400+ 行生产级代码)
