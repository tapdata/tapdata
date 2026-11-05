package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

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
    private McpSyncRequestContext context;

    private UpdateFieldDescription updateFieldDescription;

    @BeforeEach
    void setUp() {
        updateFieldDescription = new UpdateFieldDescription(new McpToolSupport(sessionAttribute, userService), metadataInstancesService);
    }

    @Test
    void testCallSuccessWithFieldId() {
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            doNothing().when(metadataInstancesService).updateTableFieldDesc(any(), any(), any());

            UpdateFieldDescription.FieldDescriptionUpdate field1 = field("field123", null, "This is a business description");

            Map<String, Object> result = updateFieldDescription.updateFieldDescription(context,
                    "507f1f77bcf86cd799439011",
                    Arrays.asList(field1));

            assertNotNull(result);
            assertEquals(true, result.get("success"));

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

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            doNothing().when(metadataInstancesService).batchUpdateTableFieldDescByName(any(), any(), any());

            UpdateFieldDescription.FieldDescriptionUpdate field1 = field(null, "username", "User login name");
            UpdateFieldDescription.FieldDescriptionUpdate field2 = field(null, "email", "User email address");

            Map<String, Object> result = updateFieldDescription.updateFieldDescription(context,
                    "507f1f77bcf86cd799439011",
                    Arrays.asList(field1, field2));

            assertNotNull(result);
            assertEquals(2, result.get("updatedCount"));

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
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            UpdateFieldDescription.FieldDescriptionUpdate field1 = field(null, "username", "desc");

            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> updateFieldDescription.updateFieldDescription(context, null, Arrays.asList(field1)));
            assertTrue(exception.getMessage().contains("metadataId"));
        }
    }

    @Test
    void testCallWithEmptyFields() {
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> updateFieldDescription.updateFieldDescription(context, "507f1f77bcf86cd799439011", new ArrayList<>()));
            assertTrue(exception.getMessage().contains("fields"));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        UpdateFieldDescription.FieldDescriptionUpdate field1 = field(null, "username", "desc");

        assertThrows(RuntimeException.class,
                () -> updateFieldDescription.updateFieldDescription(context, "507f1f77bcf86cd799439011", Arrays.asList(field1)));
    }

    private UpdateFieldDescription.FieldDescriptionUpdate field(String fieldId, String fieldName, String businessDesc) {
        UpdateFieldDescription.FieldDescriptionUpdate field = new UpdateFieldDescription.FieldDescriptionUpdate();
        field.fieldId = fieldId;
        field.fieldName = fieldName;
        field.businessDesc = businessDesc;
        return field;
    }
}
