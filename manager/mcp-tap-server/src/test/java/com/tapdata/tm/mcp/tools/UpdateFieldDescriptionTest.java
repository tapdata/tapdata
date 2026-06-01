package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author Feynman
 * @date 2025/05/19
 */
@ExtendWith(MockitoExtension.class)
class UpdateFieldDescriptionTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private MetadataInstancesService metadataInstancesService;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncServerExchange exchange;

    private UpdateFieldDescription updateFieldDescription;

    @BeforeEach
    void setUp() {
        updateFieldDescription = new UpdateFieldDescription(sessionAttribute, metadataInstancesService, userService);
    }

    @Test
    void testCallSuccessWithFieldId() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        McpServerSession mockSession = mock(McpServerSession.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            doNothing().when(metadataInstancesService).updateTableFieldDesc(any(), any(), any());

            // Build params with fields array
            Map<String, Object> field1 = new HashMap<>();
            field1.put("fieldId", "field123");
            field1.put("businessDesc", "This is a business description");

            Map<String, Object> params = new HashMap<>();
            params.put("metadataId", "507f1f77bcf86cd799439011");
            params.put("fields", Arrays.asList(field1));

            McpSchema.CallToolResult result = updateFieldDescription.call(exchange, params);

            assertNotNull(result);
            assertFalse(result.isError() != null && result.isError());

            ArgumentCaptor<DiscoveryFieldDto> fieldDtoCaptor = ArgumentCaptor.forClass(DiscoveryFieldDto.class);
            verify(metadataInstancesService).updateTableFieldDesc(
                    eq("507f1f77bcf86cd799439011"),
                    fieldDtoCaptor.capture(),
                    eq(mockUserDetail)
            );

            DiscoveryFieldDto capturedDto = fieldDtoCaptor.getValue();
            assertEquals("field123", capturedDto.getId());
            assertEquals("This is a business description", capturedDto.getBusinessDesc());
        }
    }

    @Test
    void testCallBatchWithFieldNames() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        McpServerSession mockSession = mock(McpServerSession.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            doNothing().when(metadataInstancesService).batchUpdateTableFieldDescByName(any(), any(), any());

            // Build params with multiple fields using fieldName
            Map<String, Object> field1 = new HashMap<>();
            field1.put("fieldName", "username");
            field1.put("businessDesc", "User login name");

            Map<String, Object> field2 = new HashMap<>();
            field2.put("fieldName", "email");
            field2.put("businessDesc", "User email address");

            Map<String, Object> params = new HashMap<>();
            params.put("metadataId", "507f1f77bcf86cd799439011");
            params.put("fields", Arrays.asList(field1, field2));

            McpSchema.CallToolResult result = updateFieldDescription.call(exchange, params);

            assertNotNull(result);
            assertFalse(result.isError() != null && result.isError());

            // Verify batch update was called
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<String, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
            verify(metadataInstancesService).batchUpdateTableFieldDescByName(
                    eq("507f1f77bcf86cd799439011"),
                    mapCaptor.capture(),
                    eq(mockUserDetail)
            );

            Map<String, String> capturedMap = mapCaptor.getValue();
            assertEquals(2, capturedMap.size());
            assertEquals("User login name", capturedMap.get("username"));
            assertEquals("User email address", capturedMap.get("email"));
        }
    }

    @Test
    void testCallWithoutMetadataId() {
        McpServerSession mockSession = mock(McpServerSession.class);
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            Map<String, Object> field1 = new HashMap<>();
            field1.put("fieldName", "username");
            field1.put("businessDesc", "desc");

            Map<String, Object> params = new HashMap<>();
            params.put("fields", Arrays.asList(field1));

            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> updateFieldDescription.call(exchange, params));
            assertTrue(exception.getMessage().contains("metadataId"));
        }
    }

    @Test
    void testCallWithEmptyFields() {
        McpServerSession mockSession = mock(McpServerSession.class);
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            Map<String, Object> params = new HashMap<>();
            params.put("metadataId", "507f1f77bcf86cd799439011");
            params.put("fields", new ArrayList<>());

            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> updateFieldDescription.call(exchange, params));
            assertTrue(exception.getMessage().contains("fields"));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        Map<String, Object> field1 = new HashMap<>();
        field1.put("fieldName", "username");
        field1.put("businessDesc", "desc");

        Map<String, Object> params = new HashMap<>();
        params.put("metadataId", "507f1f77bcf86cd799439011");
        params.put("fields", Arrays.asList(field1));

        assertThrows(RuntimeException.class, () -> updateFieldDescription.call(exchange, params));
    }
}
