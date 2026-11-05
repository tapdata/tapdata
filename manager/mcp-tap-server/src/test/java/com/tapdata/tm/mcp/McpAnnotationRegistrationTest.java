package com.tapdata.tm.mcp;

import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.mcp.resource.ConnectionResource;
import com.tapdata.tm.mcp.tools.CreateConnection;
import com.tapdata.tm.mcp.tools.CreateMergeTableTask;
import com.tapdata.tm.mcp.tools.ListConnection;
import com.tapdata.tm.mcp.tools.ListDataModel;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Test;
import org.springframework.ai.mcp.annotation.spring.SyncMcpAnnotationProviders;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class McpAnnotationRegistrationTest {

    @Test
    void shouldRegisterToolsFromMcpToolAnnotations() {
        List<Object> tools = List.of(
                new ListConnection(mock(McpToolSupport.class), mock(DataSourceService.class)),
                new ListDataModel(mock(McpToolSupport.class), mock(MetadataInstancesService.class)),
                new CreateMergeTableTask(mock(McpToolSupport.class), mock(TaskService.class),
                        mock(TaskSaveService.class), mock(ExternalStorageService.class), mock(DataSourceService.class))
        );

        List<McpServerFeatures.SyncToolSpecification> specs = SyncMcpAnnotationProviders.toolSpecifications(tools);

        assertEquals(3, specs.size());
        assertTrue(specs.stream().anyMatch(spec -> "listConnection".equals(spec.tool().name())));
        assertTrue(specs.stream().anyMatch(spec -> "listDataModel".equals(spec.tool().name())));
        assertTrue(specs.stream().anyMatch(spec -> "createMergeTableTask".equals(spec.tool().name())));

        Map<String, Object> listDataModelProperties = findTool(specs, "listDataModel").tool().inputSchema();
        assertProperty(listDataModelProperties, "connectionId");
        assertProperty(listDataModelProperties, "includeFields");
        assertProperty(listDataModelProperties, "name");
        assertRequired(listDataModelProperties, "connectionId");
        assertNotRequired(listDataModelProperties, "includeFields");

        Map<String, Object> createMergeProperties = findTool(specs, "createMergeTableTask").tool().inputSchema();
        assertProperty(createMergeProperties, "mainTable");
        assertProperty(createMergeProperties, "childTables");
        assertNestedProperty(createMergeProperties, "mainTable", "tableName");
    }

    @Test
    void shouldUseSpringAiCallbackToConvertBusinessReturnValue() {
        McpToolSupport toolSupport = mock(McpToolSupport.class);
        DataSourceDefinitionService dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        UserDetail userDetail = mock(UserDetail.class);
        DataSourceTypeDto mongoType = new DataSourceTypeDto();
        mongoType.setName("MongoDB");
        mongoType.setRealName("MongoDB");
        mongoType.setType("mongodb");
        mongoType.setPdkId("mongodb");
        mongoType.setConnectionType("source_and_target");
        mongoType.setProperties(Map.of("connection", Map.of("properties", Map.of(
                "host", Map.of("title", "Host", "type", "string", "required", true, "x-index", 1)
        ))));

        when(toolSupport.getUserDetail(any())).thenReturn(userDetail);
        when(dataSourceDefinitionService.dataSourceTypes(eq(userDetail), any()))
                .thenReturn(List.of(mongoType));

        CreateConnection createConnection = new CreateConnection(toolSupport, mock(DataSourceService.class), dataSourceDefinitionService);
        McpServerFeatures.SyncToolSpecification spec = findTool(
                SyncMcpAnnotationProviders.toolSpecifications(List.of(createConnection)), "createConnection");

        McpSchema.CallToolResult result = spec.callHandler().apply(mock(McpSyncServerExchange.class),
                new McpSchema.CallToolRequest("createConnection", Map.of("dataSourceType", "mongo")));

        assertFalse(Boolean.TRUE.equals(result.isError()));
        assertInstanceOf(McpSchema.TextContent.class, result.content().get(0));
        assertTrue(((McpSchema.TextContent) result.content().get(0)).text().contains("\"resolvedType\""));
    }

    @Test
    void shouldRegisterResourceAndResourceTemplatesFromMcpResourceAnnotations() {
        ConnectionResource resource = new ConnectionResource(mock(McpToolSupport.class), mock(DataSourceService.class));

        List<McpServerFeatures.SyncResourceSpecification> resourceSpecs =
                SyncMcpAnnotationProviders.resourceSpecifications(List.of(resource));
        List<McpServerFeatures.SyncResourceTemplateSpecification> templateSpecs =
                SyncMcpAnnotationProviders.resourceTemplateSpecifications(List.of(resource));

        assertEquals(1, resourceSpecs.size());
        assertEquals("tap://connections", resourceSpecs.get(0).resource().uri());
        assertEquals("Connections", resourceSpecs.get(0).resource().name());

        assertEquals(2, templateSpecs.size());
        assertTrue(templateSpecs.stream().anyMatch(spec -> "tap://{connectionId}".equals(spec.resourceTemplate().uriTemplate())));
        assertTrue(templateSpecs.stream().anyMatch(spec -> "tap://{connectionId}/{dataModelId}".equals(spec.resourceTemplate().uriTemplate())));
    }

    private McpServerFeatures.SyncToolSpecification findTool(List<McpServerFeatures.SyncToolSpecification> specs, String name) {
        return specs.stream()
                .filter(spec -> name.equals(spec.tool().name()))
                .findFirst()
                .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private void assertProperty(Map<String, Object> schema, String property) {
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        assertNotNull(properties);
        assertTrue(properties.containsKey(property), "Missing property: " + property);
    }

    @SuppressWarnings("unchecked")
    private void assertRequired(Map<String, Object> schema, String property) {
        List<String> required = (List<String>) schema.get("required");
        assertNotNull(required);
        assertTrue(required.contains(property), "Expected required property: " + property);
    }

    @SuppressWarnings("unchecked")
    private void assertNotRequired(Map<String, Object> schema, String property) {
        List<String> required = (List<String>) schema.get("required");
        if (required != null) {
            assertFalse(required.contains(property), "Expected optional property: " + property);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertNestedProperty(Map<String, Object> schema, String property, String nestedProperty) {
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        assertNotNull(properties);
        Map<String, Object> fieldSchema = (Map<String, Object>) properties.get(property);
        assertNotNull(fieldSchema);
        Map<String, Object> nestedProperties = (Map<String, Object>) fieldSchema.get("properties");
        assertNotNull(nestedProperties);
        assertTrue(nestedProperties.containsKey(nestedProperty), "Missing nested property: " + property + "." + nestedProperty);
    }
}
