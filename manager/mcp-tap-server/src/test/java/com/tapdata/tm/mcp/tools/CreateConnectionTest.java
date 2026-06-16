package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateConnectionTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private DataSourceDefinitionService dataSourceDefinitionService;

    @Mock
    private McpSyncServerExchange exchange;

    private CreateConnection createConnection;

    @BeforeEach
    void setUp() {
        createConnection = new CreateConnection(sessionAttribute, userService, dataSourceService, dataSourceDefinitionService);
    }

    @Test
    void testCallDiscoveryModeReturnsResolvedTypeAndConfigFields() {
        UserDetail userDetail = mockUser();
        DataSourceTypeDto mongoType = mongoType();

        when(dataSourceDefinitionService.dataSourceTypes(eq(userDetail), any(Filter.class)))
                .thenReturn(Collections.singletonList(mongoType));

        Map<String, Object> params = new HashMap<>();
        params.put("dataSourceType", "mongo");

        McpSchema.CallToolResult result = createConnection.call(exchange, params);

        assertNotNull(result);
        assertFalse(Boolean.TRUE.equals(result.isError()));
        String text = text(result);
        assertTrue(text.contains("\"resolvedType\""));
        assertTrue(text.contains("\"type\" : \"mongodb\""));
        assertTrue(text.contains("\"name\" : \"host\""));
        assertTrue(text.contains("\"advanced\" : false"));
        assertTrue(text.contains("\"name\" : \"ssl\""));
        assertTrue(text.contains("\"advanced\" : true"));
        assertFalse(text.contains("\"name\" : \"tip\""));
        verify(dataSourceService, never()).addConnection(any(DataSourceConnectionDto.class), eq(userDetail));
    }

    @Test
    void testCallCreateMongoConnectionWithUriSetsIsUri() {
        UserDetail userDetail = mockUser();
        DataSourceTypeDto mongoType = mongoType();
        ObjectId createdId = new ObjectId();
        DataSourceConnectionDto created = new DataSourceConnectionDto();
        created.setId(createdId);
        created.setName("Mongo Conn");
        created.setDatabase_type("mongodb");
        created.setConnection_type("source_and_target");
        created.setStatus(DataSourceConnectionDto.STATUS_TESTING);

        when(dataSourceDefinitionService.dataSourceTypes(eq(userDetail), any(Filter.class)))
                .thenReturn(Collections.singletonList(mongoType));
        when(dataSourceService.addConnection(any(DataSourceConnectionDto.class), eq(userDetail))).thenReturn(created);

        Map<String, Object> config = new HashMap<>();
        config.put("uri", "mongodb://localhost:27017/test");
        Map<String, Object> params = new HashMap<>();
        params.put("dataSourceType", "MongoDB");
        params.put("connectionName", "Mongo Conn");
        params.put("config", config);

        McpSchema.CallToolResult result = createConnection.call(exchange, params);

        assertNotNull(result);
        assertTrue(text(result).contains(createdId.toHexString()));
        ArgumentCaptor<DataSourceConnectionDto> captor = ArgumentCaptor.forClass(DataSourceConnectionDto.class);
        verify(dataSourceService).addConnection(captor.capture(), eq(userDetail));

        DataSourceConnectionDto connection = captor.getValue();
        assertEquals("Mongo Conn", connection.getName());
        assertEquals("mongodb", connection.getDatabase_type());
        assertEquals("source_and_target", connection.getConnection_type());
        assertEquals(CreateTypeEnum.User, connection.getCreateType());
        assertEquals(Boolean.TRUE, connection.getSubmit());
        assertEquals(Boolean.TRUE, connection.getUpdateSchema());
        assertEquals(Boolean.TRUE, connection.getConfig().get("isUri"));
    }

    @Test
    void testCallUnsupportedTypeThrows() {
        mockUser();
        Map<String, Object> params = new HashMap<>();
        params.put("dataSourceType", "sqlite");

        RuntimeException exception = assertThrows(RuntimeException.class, () -> createConnection.call(exchange, params));

        assertTrue(exception.getMessage().contains("Unsupported or unrecognized data source type"));
        verify(dataSourceDefinitionService, never()).dataSourceTypes(any(UserDetail.class), any(Filter.class));
        verify(dataSourceService, never()).addConnection(any(DataSourceConnectionDto.class), any(UserDetail.class));
    }

    private UserDetail mockUser() {
        UserDetail userDetail = mock(UserDetail.class);
        when(exchange.sessionId()).thenReturn("session-1");
        when(sessionAttribute.getAttribute("session-1", "userId")).thenReturn(new ObjectId().toHexString());
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);
        return userDetail;
    }

    private DataSourceTypeDto mongoType() {
        DataSourceTypeDto type = new DataSourceTypeDto();
        type.setName("MongoDB");
        type.setRealName("MongoDB");
        type.setType("mongodb");
        type.setPdkId("mongodb");
        type.setConnectionType("source_and_target");
        type.setProperties(Map.of("connection", Map.of("properties", connectionProperties())));
        return type;
    }

    private Map<String, Object> connectionProperties() {
        Map<String, Object> connectionProps = new LinkedHashMap<>();
        connectionProps.put("tip", field("Tip", "void", false, 0));
        connectionProps.put("host", field("Host", "string", true, 1));
        connectionProps.put("OPTIONAL_FIELDS", Map.of("properties", Map.of(
                "ssl", field("SSL", "boolean", false, 2)
        )));
        return connectionProps;
    }

    private Map<String, Object> field(String title, String type, boolean required, int index) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("title", title);
        field.put("type", type);
        field.put("required", required);
        field.put("x-index", index);
        return field;
    }

    private String text(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
