package com.tapdata.tm.group.handler;

import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.dto.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ResourceHandlerRegistry
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ResourceHandlerRegistry Tests")
public class ResourceHandlerRegistryTest {

    private ResourceHandlerRegistry registry;

    @Mock
    private TaskResourceHandler taskResourceHandler;

    @Mock
    private ModuleResourceHandler moduleResourceHandler;

    private UserDetail user;

    @BeforeEach
    void setUp() {
        registry = new ResourceHandlerRegistry();

        user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
                "accessCode", false, false, false, false,
                Arrays.asList(new SimpleGrantedAuthority("role")));
    }

    @Nested
    @DisplayName("getHandler Tests")
    class GetHandlerTests {

        @Test
        @DisplayName("Should return null for unregistered type")
        void testGetHandlerNotFound() {
            ResourceHandler result = registry.getHandler(ResourceType.INSPECT_TASK);
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("Handler Registration via PostConstruct")
    class PostConstructTests {

        @Test
        @DisplayName("Registry should be empty before initialization")
        void testEmptyBeforeInit() {
            ResourceHandlerRegistry emptyRegistry = new ResourceHandlerRegistry();

            // All handlers should be null before Spring initialization
            assertNull(emptyRegistry.getHandler(ResourceType.SYNC_TASK));
            assertNull(emptyRegistry.getHandler(ResourceType.MIGRATE_TASK));
            assertNull(emptyRegistry.getHandler(ResourceType.CONNECTION));
        }
    }
}
