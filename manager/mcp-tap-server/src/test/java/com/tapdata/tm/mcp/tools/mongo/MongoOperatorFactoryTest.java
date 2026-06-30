package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoOperatorFactoryTest {

    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncRequestContext context;

    private MongoOperatorFactory mongoOperatorFactory;

    @BeforeEach
    void setUp() {
        mongoOperatorFactory = new MongoOperatorFactory(new McpToolSupport(sessionAttribute, userService), dataSourceService);
    }

    @Test
    void testCreateWithValidConnection() {
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mockUser();
        when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

        MongoOperator operator = mongoOperatorFactory.create(context, connectionId);

        assertNotNull(operator);
        verify(dataSourceService).findOne(any(Query.class), eq(mockUserDetail));
    }

    @Test
    void testCreateWithoutConnectionId() {
        mockUser();
        assertThrows(RuntimeException.class, () -> mongoOperatorFactory.create(context, null));
    }

    @Test
    void testCreateNotMongoDBConnection() {
        UserDetail mockUserDetail = mockUser();
        when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mock(DataSourceConnectionDto.class));

        assertThrows(RuntimeException.class, () -> mongoOperatorFactory.create(context, "507f1f77bcf86cd799439011"));
    }

    @Test
    void testCreateWithNonMongoConnection() {
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mysql");
        UserDetail mockUserDetail = mockUser();
        when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

        assertThrows(RuntimeException.class, () -> mongoOperatorFactory.create(context, connectionId));
    }

    @Test
    void testCreateWithNonExistentConnection() {
        String connectionId = "507f1f77bcf86cd799439011";
        UserDetail mockUserDetail = mockUser();
        when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(null);

        assertThrows(RuntimeException.class, () -> mongoOperatorFactory.create(context, connectionId));
    }

    private UserDetail mockUser() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        when(context.sessionId()).thenReturn("session-1");
        when(sessionAttribute.getAttribute("session-1", "userId")).thenReturn("507f1f77bcf86cd799439012");
        when(userService.loadUserById(any())).thenReturn(mockUserDetail);
        return mockUserDetail;
    }
}
