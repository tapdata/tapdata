package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2025/04/21 09:08
 */
@ExtendWith(MockitoExtension.class)
class ResourceTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncServerExchange exchange;

    private TestResource resource;

    @BeforeEach
    void setUp() {
        resource = new TestResource(sessionAttribute, userService);
    }

    @Test
    void testGetUserIdFromExchange() {
        String sessionId = "test-session-id";
        String userId = "test-user-id";
        when(exchange.sessionId()).thenReturn(sessionId);
        when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);

        String result = resource.getUserId(exchange);

        assertEquals(userId, result);
        verify(sessionAttribute).getAttribute(sessionId, "userId");
    }

    @Test
    void testGetUserIdFromSessionId() {
        String sessionId = "test-session-id";
        String userId = "test-user-id";

        when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);

        String result = resource.getUserId(sessionId);
        assertEquals(userId, result);

        verify(sessionAttribute).getAttribute(sessionId, "userId");
    }

    @Test
    void testGetUserIdWithNullSessionAttribute() {
        resource = new TestResource(null, userService);
        
        Exception exception = assertThrows(RuntimeException.class, () -> {
            resource.getUserId("test-session-id");
        });
        
        assertEquals("Not initialized sessionAttribute before call.", exception.getMessage());
    }

    @Test
    void testGetUserDetail() {
        String sessionId = "test-session-id";
        String userId = "507f1f77bcf86cd799439011";
        ObjectId objectId = new ObjectId(userId);
        UserDetail mockUserDetail = mock(UserDetail.class);

        when(exchange.sessionId()).thenReturn(sessionId);
        when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);
        when(userService.loadUserById(objectId)).thenReturn(mockUserDetail);

        UserDetail result = resource.getUserDetail(exchange);

        assertSame(mockUserDetail, result);
        verify(userService).loadUserById(objectId);
    }

    @Test
    void testGetUserDetailWithNullUserId() {
        String sessionId = "test-session-id";

        when(exchange.sessionId()).thenReturn(sessionId);
        when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(null);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            resource.getUserDetail(exchange);
        });

        assertEquals("Not found userId in current session", exception.getMessage());
    }

    @Test
    void testGetUserDetailWithNullUserService() {
        String sessionId = "test-session-id";
        String userId = "test-user-id";
        resource = new TestResource(sessionAttribute, null);

        when(exchange.sessionId()).thenReturn(sessionId);
        when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            resource.getUserDetail(exchange);
        });

        assertEquals("Not initialized userServices before call.", exception.getMessage());
    }

    // Test implementation of Resource for testing
    private static class TestResource extends Resource {
        public TestResource(SessionAttribute sessionAttribute, UserService userService) {
            super("test-uri", "test-name", "test-description", "test/mime-type", 
                  null, sessionAttribute, userService);
        }

        @Override
        public McpSchema.ReadResourceResult call(McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request) {
            return new McpSchema.ReadResourceResult(Collections.emptyList());
        }
    }
}
