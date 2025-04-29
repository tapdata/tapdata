package com.tapdata.tm.mcp;

import com.tapdata.tm.mcp.resource.Resource;
import com.tapdata.tm.mcp.tools.Tool;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2024/4/24 10:09
 */
@ExtendWith(MockitoExtension.class)
class McpTapServerTest {

    @Mock
    private SseServerTransportProvider transportProvider;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private McpSyncServer mcpServer;

    @Mock
    private Resource mockResource;

    @Mock
    private Tool mockTool;

    private McpTapServer mcpTapServer;

    @BeforeEach
    void setUp() {
        mcpTapServer = new McpTapServer(transportProvider, applicationContext);
    }

    @Test
    void testAfterPropertiesSet() throws Exception {
        // 准备测试数据
        String resourceName = "testResource";
        String toolName = "testTool";
        String[] resourceBeanNames = new String[]{resourceName};
        String[] toolBeanNames = new String[]{toolName};

        // 设置 mock 行为
        try (MockedStatic<McpServer> mcpServerMock = mockStatic(McpServer.class)) {
            // Mock McpServer.sync() 方法
            McpServer.SyncSpecification mockSyncSpecification = mock(McpServer.SyncSpecification.class);
            when(mockSyncSpecification.serverInfo(anyString(), anyString())).thenReturn(mockSyncSpecification);
            when(mockSyncSpecification.capabilities(any())).thenReturn(mockSyncSpecification);
            when(mockSyncSpecification.resourceTemplates(anyList())).thenReturn(mockSyncSpecification);
            when(mockSyncSpecification.build()).thenReturn(mcpServer);
            mcpServerMock.when(() -> McpServer.sync(any())).thenReturn(mockSyncSpecification);

            // Mock Resource
            when(applicationContext.getBeanNamesForType(Resource.class)).thenReturn(resourceBeanNames);
            when(applicationContext.getBean(resourceName)).thenReturn(mockResource);
            when(mockResource.getUri()).thenReturn("test://uri");
            when(mockResource.getName()).thenReturn("Test Resource");
            when(mockResource.getDescription()).thenReturn("Test Description");
            when(mockResource.getMimeType()).thenReturn("application/json");

            // Mock Tool
            when(applicationContext.getBeanNamesForType(Tool.class)).thenReturn(toolBeanNames);
            when(applicationContext.getBean(toolName)).thenReturn(mockTool);
            when(mockTool.getName()).thenReturn("Test Tool");
            when(mockTool.getDescription()).thenReturn("Test Tool Description");
            when(mockTool.getInputSchema()).thenReturn("{}");

            // 执行测试
            mcpTapServer.afterPropertiesSet();

            // 验证 Resource 相关调用
            verify(applicationContext).getBeanNamesForType(Resource.class);
            verify(applicationContext).getBean(resourceName);
            verify(mockResource).getUri();
            verify(mockResource).getName();
            verify(mockResource).getDescription();
            verify(mockResource).getMimeType();

            // 验证 Tool 相关调用
            verify(applicationContext).getBeanNamesForType(Tool.class);
            verify(applicationContext).getBean(toolName);
            verify(mockTool).getName();
            verify(mockTool).getDescription();
            verify(mockTool, times(2)).getInputSchema();
        }
    }

    @Test
    void testGetResourceTemplate() throws Exception {
        // 使用反射调用私有方法
        var method = McpTapServer.class.getDeclaredMethod("getResourceTemplate");
        method.setAccessible(true);
        
        // 执行测试
        @SuppressWarnings("unchecked")
        List<McpSchema.ResourceTemplate> templates = (List<McpSchema.ResourceTemplate>) method.invoke(mcpTapServer);
        
        // 验证结果
        assert templates != null;
        assert templates.size() == 2;
        
        // 验证第一个模板
        McpSchema.ResourceTemplate connectionTemplate = templates.get(0);
        assert "tap://{connectionId}".equals(connectionTemplate.uriTemplate());
        assert "Connection".equals(connectionTemplate.name());
        assert "Available database connections in TapData".equals(connectionTemplate.description());
        assert "application/json".equals(connectionTemplate.mimeType());
        
        // 验证第二个模板
        McpSchema.ResourceTemplate dataModelTemplate = templates.get(1);
        assert "tap://{connectionId}/{dataModelId}".equals(dataModelTemplate.uriTemplate());
        assert "DataModel".equals(dataModelTemplate.name());
        assert "Data model loaded by connection in TapData".equals(dataModelTemplate.description());
        assert "application/json".equals(dataModelTemplate.mimeType());
    }

    @Test
    void testAddResources() throws Exception {
        // 准备测试数据
        String resourceName = "testResource";
        String[] resourceBeanNames = new String[]{resourceName};

        // 设置 mock 行为
        when(applicationContext.getBeanNamesForType(Resource.class)).thenReturn(resourceBeanNames);
        when(applicationContext.getBean(resourceName)).thenReturn(mockResource);
        when(mockResource.getUri()).thenReturn("test://uri");
        when(mockResource.getName()).thenReturn("Test Resource");
        when(mockResource.getDescription()).thenReturn("Test Description");
        when(mockResource.getMimeType()).thenReturn("application/json");

        // 注入 mock 的 mcpServer
        var field = McpTapServer.class.getDeclaredField("mcpServer");
        field.setAccessible(true);
        field.set(mcpTapServer, mcpServer);

        // 执行测试
        var method = McpTapServer.class.getDeclaredMethod("addResources");
        method.setAccessible(true);
        method.invoke(mcpTapServer);

        // 验证调用
        verify(mcpServer).addResource(any(McpServerFeatures.SyncResourceSpecification.class));
    }

    @Test
    void testAddTools() throws Exception {
        // 准备测试数据
        String toolName = "testTool";
        String[] toolBeanNames = new String[]{toolName};

        // 设置 mock 行为
        when(applicationContext.getBeanNamesForType(Tool.class)).thenReturn(toolBeanNames);
        when(applicationContext.getBean(toolName)).thenReturn(mockTool);
        when(mockTool.getName()).thenReturn("Test Tool");
        when(mockTool.getDescription()).thenReturn("Test Tool Description");
        when(mockTool.getInputSchema()).thenReturn("{}");

        // 注入 mock 的 mcpServer
        ReflectionTestUtils.setField(mcpTapServer, "mcpServer", mcpServer);

        try (MockedConstruction<McpServerFeatures.SyncToolSpecification> mockSyncToolSpec = mockConstruction(McpServerFeatures.SyncToolSpecification.class, (mock, context) -> {
            Object tool = context.arguments().get(0);
            BiFunction<McpSyncServerExchange, Map<String, Object>, McpSchema.CallToolResult> call = (BiFunction<McpSyncServerExchange, Map<String, Object>, McpSchema.CallToolResult>) context.arguments().get(1);

            // 验证参数
            Assertions.assertNotNull(tool);
            Assertions.assertNotNull(call);
            Assertions.assertInstanceOf(McpSchema.Tool.class, tool);
            Assertions.assertEquals(mockTool.getName(), ((McpSchema.Tool)tool).name());

            // 执行工具调用
            call.apply(null, null);
        })) {

            // 执行测试
            ReflectionTestUtils.invokeMethod(mcpTapServer, "addTools");

            // 验证调用
            verify(mcpServer).addTool(any(McpServerFeatures.SyncToolSpecification.class));
            verify(mockTool).call(any(), any());

        }
    }

    @Test
    void testAddToolsWithNullSchema() throws Exception {
        // 准备测试数据
        String toolName = "testTool";
        String[] toolBeanNames = new String[]{toolName};

        // 设置 mock 行为
        when(applicationContext.getBeanNamesForType(Tool.class)).thenReturn(toolBeanNames);
        when(applicationContext.getBean(toolName)).thenReturn(mockTool);
        when(mockTool.getName()).thenReturn("Test Tool");
        when(mockTool.getInputSchema()).thenReturn(null);
        when(mockTool.getJsonSchema()).thenReturn(null);

        // 注入 mock 的 mcpServer
        var field = McpTapServer.class.getDeclaredField("mcpServer");
        field.setAccessible(true);
        field.set(mcpTapServer, mcpServer);

        // 执行测试
        var method = McpTapServer.class.getDeclaredMethod("addTools");
        method.setAccessible(true);
        method.invoke(mcpTapServer);

        // 验证调用 - 不应该添加没有 schema 的工具
        verify(mcpServer, never()).addTool(any(McpServerFeatures.SyncToolSpecification.class));
    }
} 