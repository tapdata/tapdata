# SQL 自动解析功能设计文档

## 目标

实现基于 JSqlParser 的 SQL 自动解析器，解析用户 SELECT SQL 中的表信息和 JOIN 关系，自动生成 `customJoinQueries` 格式的查询模板，减少用户手动配置负担。

## 架构概述

使用项目已有的 JSqlParser 库（CCJSqlParserUtil）解析 SQL，创建 `SqlJoinAnalyzer` 类提取所有表和 JOIN 关系，生成受影响主键计算所需的 JOIN 查询模板。解析失败时回退到用户配置的 `customJoinQueries`。

## 技术栈

- **JSqlParser**：SQL 解析库（项目已有依赖）
- **Java 17**：开发语言
- **JUnit 5 + Mockito**：测试框架

## 核心组件

### 1. SqlJoinAnalyzer

**职责**：解析用户 SQL，提取表信息和 JOIN 关系，生成 JOIN 查询模板

**位置**：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SqlJoinAnalyzer.java`

### 2. SqlJoinAnalysis

**职责**：封装 SQL 解析结果

**位置**：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SqlJoinAnalysis.java`

### 3. TableInfo

**职责**：封装单个表的信息

**位置**：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/TableInfo.java`

### 4. JoinRelation

**职责**：封装两个表之间的 JOIN 关系

**位置**：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/JoinRelation.java`

## 数据流程

```
User SQL → SqlJoinAnalyzer.analyze() → SqlJoinAnalysis
    → 提取所有表（含 CTE、子查询）
    → 提取 JOIN 关系（ON 条件 + WHERE 隐式关联）
    → 生成 customJoinQueries 格式模板
    → 合并用户配置（用户配置优先）
    → AffectedKeyCalculator 使用
```

## 错误处理策略

| 场景 | 处理方式 |
|------|----------|
| SQL 语法错误 | 记录错误日志，回退到 `customJoinQueries` |
| 无法解析 JOIN 条件 | 记录警告日志，回退到 `customJoinQueries` |
| 无法推断主键 | 使用默认 "id"，记录警告日志 |
| 表不存在于配置中 | 返回空集合，记录调试日志 |

## 测试策略

覆盖简单单表、显式 JOIN、隐式 JOIN、LEFT/RIGHT/FULL JOIN、CTE、子查询、解析失败回退等场景。
