# TapData MCP Server

`mcp-tap-server` 基于 Spring AI MCP Server WebMVC Starter 提供 TapData MCP 能力。它随 TapData Manager 主应用启动，由主应用 Spring 上下文加载，不作为独立服务进程运行。

当前协议为 Streamable HTTP，默认端点为 `/mcp`。主能力只启用 tool 和 resource，prompt、completion 关闭。

## 当前状态

MCP 模块已经切到 Spring AI 注解式注册，整体比之前直接对接 `modelcontextprotocol` SDK 更清晰：

- Tool 使用 `@McpTool`、`@McpToolParam` 注册。
- Resource 使用 `@McpResource` 注册。
- 参数 JSON Schema 由 Spring AI 根据 Java 方法签名和注解自动生成。
- 不再维护独立 `*.json` schema 文件。
- 不再保留 `Tool`、`MongoTool`、`Resource` 这类注册用继承基类。
- 不再手写 `SyncToolSpecification` 注册 tool。
- 不再保留自定义 `McpTapServer` 和自定义 `StreamableMcpTransportProvider`。
- 不暴露 `@McpPrompt`，主应用配置中 `prompt: false`。

仍然保留少量 `io.modelcontextprotocol.*` 类型，但只在协议边界使用，例如 transport 装配、MCP header、session transport context 和注解注册测试。业务 tool/resource 方法不应该直接返回或操作 `McpSchema.CallToolResult`、`McpSchema.ReadResourceResult`、`McpSyncServerExchange`。

## 启动配置

MCP server 配置放在主应用：

`manager/tm/src/main/resources/application-default.yml`

```yaml
spring:
  ai:
    mcp:
      server:
        enabled: true
        name: mcp-tap-server
        version: 1.0.0
        type: SYNC
        protocol: STREAMABLE
        capabilities:
          tool: true
          resource: true
          prompt: false
          completion: false
        streamable-http:
          mcp-endpoint: /mcp
```

不需要额外的 `application-mcp.yml`，也不需要额外激活 `mcp` profile。只要主应用加载 `mcp-tap-server` 模块，Spring AI 会根据配置初始化 MCP server。

## 模块结构

| 路径 / 类 | 作用 |
| --- | --- |
| `McpConfig` | 使用 Spring AI 官方 `WebMvcStreamableServerTransportProvider` 装配 Streamable HTTP transport，并为 RouterFunction 增加 TapData accessCode 鉴权 filter。 |
| `McpAccessCodeAuthentication` | 处理初始化请求的 accessCode 鉴权、角色校验、session 属性写入和用户日志。 |
| `McpSessionAttributes` | 保存 MCP session 与 TapData `token/userId` 的映射，并把属性暴露给 Spring AI transport context。 |
| `tools/` | TapData 业务 tools。目录只用于代码组织，Spring AI 识别的是 Spring bean 上的 `@McpTool` 方法。 |
| `tools/McpToolSupport` | tool/resource 共用辅助组件，从 `McpSyncRequestContext` 读取当前用户上下文。 |
| `tools/mongo/` | MongoDB 相关 tools。`MongoOperatorFactory` 负责按连接 ID 创建 Mongo 操作对象。 |
| `resource/` | MCP resources。当前暴露 `tap://connections`、`tap://{connectionId}`、`tap://{connectionId}/{dataModelId}`。 |
| `mongodb/` | MongoDB 操作封装，不直接参与 MCP 注册。 |
| `Utils`、`SessionAttribute`、`exception/` | 模块内通用工具、session 兼容接口和异常类型。 |

## 注册链路

Spring AI 的注册流程是：

1. Spring 扫描 `@Component` bean。
2. Spring AI 扫描 bean 方法上的 `@McpTool`、`@McpResource`。
3. 方法参数和 `@McpToolParam` 会生成 tool input schema。
4. `McpSyncRequestContext` 是 Spring AI 支持的上下文参数，不会进入 input schema。
5. 方法返回的普通 Java 对象由 Spring AI 转换成 MCP 响应。

所以新增能力时重点是写好 Spring bean、注解和方法签名，不需要关心底层 MCP `CallToolResult` 组装。

## 鉴权流程

TapData MCP 继续兼容原来的 accessCode 方式。Streamable HTTP 初始化请求不带 `Mcp-Session-Id` 时会执行鉴权。

支持的 accessCode 传入方式：

- `Authorization: Bearer <accessCode>`
- `Authorization: <accessCode>`
- `?accessCode=<accessCode>`

鉴权步骤：

1. `Utils.getAccessCode(request)` 读取 accessCode。
2. `AccessTokenService.generateToken(accessCode)` 生成 TapData token。
3. `UserService.getUserDetail(userId)` 加载用户角色。
4. 仅允许 `mcp` 或 `admin` 角色访问。
5. 初始化响应成功后，将 `token`、`userId` 写入 `McpSessionAttributes`。
6. tool/resource 通过 `McpSyncRequestContext` 和 `McpToolSupport` 读取当前用户。

## Tool 开发规范

新增 tool 时优先使用 Spring AI 原生注解。

基本规则：

- 类声明为 Spring bean，通常使用 `@Component`。
- 方法声明 `@McpTool(name = "...", description = "...")`。
- 业务参数使用 `@McpToolParam(description = "...")` 描述。
- 可选参数显式设置 `@McpToolParam(required = false, ...)`。
- 需要当前用户时，在方法参数中加入 `McpSyncRequestContext context`。
- 返回值优先使用业务 DTO、`Map`、`List` 或 `String`。
- 不要返回 `McpSchema.CallToolResult`。
- 不要新增 JSON Schema 文件。
- 不要为注册目的继承公共 `Tool` 基类。
- Tool 方法应直接根据声明参数执行业务逻辑，避免 `参数 -> JSON/Map -> 再解析`。
- 只有底层 API 本身就是文档型结构时才使用 `Map<String, Object>`，例如 Mongo filter、projection、pipeline stage。

示例：

```java
@Component
public class ListSomething {

    private final McpToolSupport toolSupport;

    public ListSomething(McpToolSupport toolSupport) {
        this.toolSupport = toolSupport;
    }

    @McpTool(name = "listSomething", description = "List something in TapData")
    public Map<String, Object> listSomething(
            McpSyncRequestContext context,
            @McpToolParam(description = "Required id.") String id,
            @McpToolParam(required = false, description = "Optional keyword.") String keyword,
            @McpToolParam(required = false, description = "Nested filter.") FilterParam filter) {

        UserDetail userDetail = toolSupport.getUserDetail(context);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userDetail.getUserId());
        result.put("id", id);
        result.put("keyword", keyword);
        result.put("filter", filter);
        return result;
    }

    public static class FilterParam {

        @McpToolParam(required = false, description = "Field name.")
        private String field;

        @McpToolParam(required = false, description = "Field value.")
        private String value;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
```

嵌套 DTO 字段上的 `@McpToolParam` 会参与 schema 生成。DTO 建议保留标准 getter/setter，减少 Jackson 和 schema 推断差异。

## Resource 开发规范

Resource 使用 `@McpResource`。普通 URI 会注册为 resource，带 `{variable}` 的 URI 会注册为 resource template。

```java
@McpResource(
        name = "Connection",
        uri = "tap://{connectionId}",
        description = "A TapData database connection",
        mimeType = "application/json")
public String getConnection(McpSyncRequestContext context, String connectionId) {
    UserDetail userDetail = toolSupport.getUserDetail(context);
    return Utils.toJson(loadConnection(connectionId, userDetail));
}
```

注意事项：

- URI 模板变量需要在方法参数中接收，例如 `String connectionId`。
- 如需用户上下文，同样传入 `McpSyncRequestContext context`。
- 返回值优先使用 `String`、`List<String>` 或可被序列化的业务对象。
- 不要直接组装 `McpSchema.ReadResourceResult`。

## 清洁边界

后续维护时可以按这个边界判断代码是否干净：

- `McpConfig` 可以接触 Spring AI transport 和少量 MCP SDK 类型。
- `McpAccessCodeAuthentication` 可以接触 MCP HTTP header，因为它处理初始化请求鉴权。
- `McpSessionAttributes` 可以创建 `McpTransportContext`，这是给 Spring AI 上下文传值的桥。
- `McpToolSupport` 可以读取 `McpSyncRequestContext`，向业务层提供 `userId/token/UserDetail`。
- 具体 tool/resource 只写业务方法、业务参数和业务返回值。
- 测试可以使用 Spring AI 生成的 MCP specification 验证注解注册结果。

如果一个业务 tool 里开始出现 `McpSchema.*`、手写 schema 字符串、手动解析 MCP request，通常就说明边界开始变脏。

## 验证

常用验证命令：

```bash
mvn -pl mcp-tap-server -DskipTests test-compile
mvn -pl mcp-tap-server -Dtest=McpAnnotationRegistrationTest,McpConfigTest,McpAccessCodeAuthenticationTest -DsurefireArgLine='-javaagent:/Users/xf/.m2/repository/net/bytebuddy/byte-buddy-agent/1.17.8/byte-buddy-agent-1.17.8.jar' test
```

在 JDK 22 环境下，Mockito inline 可能无法自附加 Byte Buddy agent；测试命令中显式指定 `byte-buddy-agent` 是为了绕过这个 JVM 限制。
