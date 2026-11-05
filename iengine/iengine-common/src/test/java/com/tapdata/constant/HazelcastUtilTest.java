package com.tapdata.constant;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.tapdata.cache.CacheInvalidationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class HazelcastUtilTest {

    private HazelcastInstance hazelcastInstance;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    @BeforeEach
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        mongoClient = mock(MongoClient.class);
        mongoDatabase = mock(MongoDatabase.class);

        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);

        // Reset the static field before each test
        HazelcastUtil.shutdownCacheInvalidationService();
    }

    @AfterEach
    void tearDown() {
        HazelcastUtil.shutdownCacheInvalidationService();
    }

    @Nested
    @DisplayName("Initialization Tests")
    class CacheInvalidationInitializationTest {

        @Test
        @DisplayName("Should initialize cache invalidation service successfully")
        void testInitCacheInvalidationService() {
            try (MockedConstruction<CacheInvalidationService> mockedConstruction = mockConstruction(
                CacheInvalidationService.class,
                (mock, context) -> {
                    // Verify constructor parameters
                    assertEquals(4, context.arguments().size());
                    assertEquals(hazelcastInstance, context.arguments().get(0));
                    assertEquals(mongoClient, context.arguments().get(1));
                    assertEquals("testDB", context.arguments().get(2));
                    assertEquals("test-node-id", context.arguments().get(3));
                }
            )) {
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

                // Verify service was created and started
                assertEquals(1, mockedConstruction.constructed().size());
                CacheInvalidationService service = mockedConstruction.constructed().get(0);
                verify(service).start();

                // Verify service is stored
                assertNotNull(HazelcastUtil.getCacheInvalidationService());
            }
        }

        @Test
        @DisplayName("Should not initialize service twice")
        void testInitCacheInvalidationServiceTwice() {
            try (MockedConstruction<CacheInvalidationService> mockedConstruction = mockConstruction(
                CacheInvalidationService.class
            )) {
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

                // Should only create one instance
                assertEquals(1, mockedConstruction.constructed().size());
            }
        }

        @Test
        @DisplayName("Should handle initialization failure")
        void testInitCacheInvalidationServiceWithException() {
            try (MockedConstruction<CacheInvalidationService> mockedConstruction = mockConstruction(
                CacheInvalidationService.class,
                (mock, context) -> {
                    doThrow(new RuntimeException("Init failed")).when(mock).start();
                }
            )) {
                assertThrows(RuntimeException.class, () ->
                    HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id")
                );
            }
        }

        @Test
        @DisplayName("Should fail when MongoClient is null")
        void testInitCacheInvalidationServiceWithNullMongoClient() {
            assertThrows(RuntimeException.class, () ->
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, null, "testDB", "test-node-id")
            );
        }
    }

    @Nested
    @DisplayName("Get Service Tests")
    class CacheInvalidationGetServiceTests {

        @Test
        @DisplayName("Should return null when service not initialized")
        void testGetCacheInvalidationServiceWhenNotInitialized() {
            assertNull(HazelcastUtil.getCacheInvalidationService());
        }

        @Test
        @DisplayName("Should return service when initialized")
        void testGetCacheInvalidationServiceWhenInitialized() {
            try (MockedConstruction<CacheInvalidationService> mockedConstruction = mockConstruction(
                CacheInvalidationService.class
            )) {
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

                CacheInvalidationService service = HazelcastUtil.getCacheInvalidationService();
                assertNotNull(service);
                assertEquals(mockedConstruction.constructed().get(0), service);
            }
        }
    }

    @Nested
    @DisplayName("Shutdown Tests")
    class CacheInvalidationShutdownTests {

        @Test
        @DisplayName("Should shutdown service successfully")
        void testShutdownCacheInvalidationService() {
            try (MockedConstruction<CacheInvalidationService> mockedConstruction = mockConstruction(
                CacheInvalidationService.class
            )) {
                HazelcastUtil.initCacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

                CacheInvalidationService service = mockedConstruction.constructed().get(0);

                HazelcastUtil.shutdownCacheInvalidationService();

                verify(service).stop();
                assertNull(HazelcastUtil.getCacheInvalidationService());
            }
        }

        @Test
        @DisplayName("Should handle shutdown when not initialized")
        void testShutdownWhenNotInitialized() {
            assertDoesNotThrow(() -> HazelcastUtil.shutdownCacheInvalidationService());
        }
    }
}
