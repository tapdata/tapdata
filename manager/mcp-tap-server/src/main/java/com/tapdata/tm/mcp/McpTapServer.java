package com.tapdata.tm.mcp;

import com.tapdata.tm.mcp.resource.Resource;
import com.tapdata.tm.mcp.tools.Tool;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Arrays;
import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/24 18:53
 */
@Component
@Slf4j
public class McpTapServer implements InitializingBean {

    private final SseServerTransportProvider transportProvider;
    private McpSyncServer mcpServer;
    private ApplicationContext applicationContext;

    public McpTapServer(SseServerTransportProvider transportProvider, ApplicationContext applicationContext) {
        this.transportProvider = transportProvider;
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.mcpServer = McpServer.sync(transportProvider)
                .serverInfo("mcp-tap-server", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder()
                        //.resources(false, false)
                        .tools(true)
                        //.prompts(false)
                        .logging()
                        .build())
                .resourceTemplates(getResourceTemplate())
                .build();

        //addResources();
        addTools();
        //addPrompts();

        // Send logging notifications
        mcpServer.loggingNotification(McpSchema.LoggingMessageNotification.builder()
                .level(McpSchema.LoggingLevel.INFO)
                .logger("mcp-tap-server")
                .data("Server initialized")
                .build());
    }

    private List<McpSchema.ResourceTemplate> getResourceTemplate() {
        return Arrays.asList(
                new McpSchema.ResourceTemplate("tap://{connectionId}", "Connection", "Available database connections in TapData", "application/json", null),
                new McpSchema.ResourceTemplate("tap://{connectionId}/{schemaId}", "DataSchema", "Data schema loaded by connection in TapData", "application/json", null)
        );
    }

    private void addResources() {
        // MCP Server SDK 升级后，不支持根据用户身份获取可访问resource，以后在添加支持
        Arrays.stream(applicationContext.getBeanNamesForType(Resource.class))
                .map(n -> (Resource)applicationContext.getBean(n)).forEach(res -> {
                    McpServerFeatures.SyncResourceSpecification syncResourceSpecification =
                            new McpServerFeatures.SyncResourceSpecification(res, res::call);
                    mcpServer.addResource(syncResourceSpecification);
                });
    }

    private void addTools() {
        Arrays.stream(applicationContext.getBeanNamesForType(Tool.class))
            .map(n -> (Tool)applicationContext.getBean(n)).forEach(tool -> {
                McpServerFeatures.SyncToolSpecification syncToolSpecification = new McpServerFeatures.SyncToolSpecification(tool, (exchange, params) -> {
                    try {
                        long start = System.currentTimeMillis();
                        McpSchema.CallToolResult result = tool.call(exchange, params);
                        log.info("Execute tool {} for {}ms ", tool.getName(), System.currentTimeMillis() - start);
                        return result;
                    } catch (RuntimeException e) {
                        log.error("Error calling tool " + tool.getName(), e);
                        throw e;
                    } catch (Exception e) {
                        log.error("Error calling tool " + tool.getName(), e);
                        throw new RuntimeException(e);
                    }
                });
                mcpServer.addTool(syncToolSpecification);
            });
    }

    private void addPrompts() {
        //mcpServer.addPrompt(syncPromptSpecification);
    }
}
