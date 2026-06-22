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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/24 18:53
 */
@Component
@Slf4j
public class McpTapServer implements InitializingBean {

    private final StreamableMcpTransportProvider transportProvider;
    private McpSyncServer mcpServer;
    private ApplicationContext applicationContext;

    public McpTapServer(StreamableMcpTransportProvider transportProvider, ApplicationContext applicationContext) {
        this.transportProvider = transportProvider;
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.mcpServer = McpServer.sync(transportProvider)
                .serverInfo("mcp-tap-server", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder()
                        .resources(true, false)
                        .tools(true)
                        .prompts(false)
                        .logging()
                        .build())
                .build();

        addResources();
        addResourceTemplates();
        addTools();
        addPrompts();
    }

    private List<McpSchema.ResourceTemplate> getResourceTemplate() {
        return Arrays.asList(
                new McpSchema.ResourceTemplate("tap://{connectionId}", "Connection", "Available database connections in TapData", "application/json", null),
                new McpSchema.ResourceTemplate("tap://{connectionId}/{dataModelId}", "DataModel", "Data model loaded by connection in TapData", "application/json", null)
        );
    }

    private void addResources() {
        Arrays.stream(applicationContext.getBeanNamesForType(Resource.class))
                .map(n -> (Resource)applicationContext.getBean(n)).forEach(res -> {
                    McpSchema.Resource resource = McpSchema.Resource.builder()
                            .uri(res.getUri())
                            .name(res.getName())
                            .description(res.getDescription())
                            .mimeType(res.getMimeType())
                            .annotations(res.getAnnotations())
                            .build();
                    McpServerFeatures.SyncResourceSpecification syncResourceSpecification =
                            new McpServerFeatures.SyncResourceSpecification(resource, res::call);
                    mcpServer.addResource(syncResourceSpecification);
                });
    }

    private void addResourceTemplates() {
        Resource resource = Arrays.stream(applicationContext.getBeanNamesForType(Resource.class))
                .map(n -> (Resource)applicationContext.getBean(n))
                .findFirst()
                .orElse(null);
        if (resource == null) {
            return;
        }
        getResourceTemplate().forEach(template -> mcpServer.addResourceTemplate(
                new McpServerFeatures.SyncResourceTemplateSpecification(template, resource::call)));
    }

    private void addTools() {
        Arrays.stream(applicationContext.getBeanNamesForType(Tool.class))
            .map(n -> (Tool)applicationContext.getBean(n)).forEach(tool -> {
                Optional<McpSchema.Tool> mcpTool = buildTool(tool);
                if (mcpTool.isEmpty()) {
                    log.error("Tool {} input schema cannot be empty, ignore register this tool", tool.getName());
                    return;
                }
                McpServerFeatures.SyncToolSpecification syncToolSpecification = new McpServerFeatures.SyncToolSpecification(mcpTool.get(), (exchange, request) -> {
                    try {
                        long start = System.currentTimeMillis();
                        McpSchema.CallToolResult result = tool.call(exchange, request.arguments());
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

    private Optional<McpSchema.Tool> buildTool(Tool tool) {
        McpSchema.Tool.Builder builder = McpSchema.Tool.builder()
                .name(tool.getName())
                .description(tool.getDescription());
        if (tool.getInputSchema() != null) {
            builder.inputSchema(io.modelcontextprotocol.json.McpJsonDefaults.getMapper(), tool.getInputSchema());
        } else if (tool.getJsonSchema() != null) {
            builder.inputSchema(tool.getJsonSchema());
        } else {
            return Optional.empty();
        }
        return Optional.of(builder.build());
    }

    private void addPrompts() {
        mcpServer.addPrompt(new McpServerFeatures.SyncPromptSpecification(
                new McpSchema.Prompt("Load data model", "Indicates the order in which the data model is loaded and the tools to be used", Collections.emptyList()), (exchange, params) -> {

                    return new McpSchema.GetPromptResult("Load data model", Arrays.asList(
                            new McpSchema.PromptMessage(McpSchema.Role.ASSISTANT, new McpSchema.TextContent(null,
                                    "When you need to load the data model, you need to follow the steps below\n" +
                                            "1. Execute listConnection to load all available database connections and obtain the database connection ID;\n" +
                                            "2. Based on the database connection ID in the previous step, execute listDataModel to obtain the data model",
                                    null))
                    ));
        }));
    }
}
