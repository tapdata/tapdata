package com.tapdata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.mongodb.client.*;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("CacheInvalidationService Test")
class CacheInvalidationServiceTest {

    private HazelcastInstance hazelcastInstance;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;
    private CacheInvalidationService service;

    @BeforeEach
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        mongoClient = mock(MongoClient.class);
        mongoDatabase = mock(MongoDatabase.class);
        mongoCollection = mock(MongoCollection.class);

        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoDatabase.runCommand(any(Document.class))).thenReturn(new Document("ok", 1));
    }

    @AfterEach
    void tearDown() {
        if (service != null && service.isRunning()) {
            service.stop();
        }
    }

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("Should create service with valid parameters")
        void testConstructorWithValidParameters() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            assertNotNull(service);
            assertFalse(service.isRunning());
        }
    }

    @Nested
    @DisplayName("Start and Stop Tests")
    class StartStopTests {

        @Test
        @DisplayName("Should start service successfully")
        void testStart() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            service.start();
            assertTrue(service.isRunning());
        }

        @Test
        @DisplayName("Should not start service twice")
        void testStartTwice() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            service.start();
            service.start(); // Second start should be ignored
            assertTrue(service.isRunning());
        }

        @Test
        @DisplayName("Should stop service successfully")
        void testStop() throws InterruptedException {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            service.start();
            assertTrue(service.isRunning());
            
            service.stop();
            // Wait a bit for the thread to stop
            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(service.isRunning());
        }

        @Test
        @DisplayName("Should handle stop when not started")
        void testStopWhenNotStarted() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            assertDoesNotThrow(() -> service.stop());
            assertFalse(service.isRunning());
        }

        @Test
        void testStopWithOwnedMongoClient() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");
            service.start();
            service.stop();

            verify(mongoClient, times(1)).close();
        }
    }

    @Nested
    @DisplayName("Publish Invalidation Tests")
    class PublishInvalidationTests {
        @Test
        @DisplayName("Should handle MongoDB exception gracefully")
        void testPublishWithMongoException() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");

            when(mongoCollection.insertOne(any(Document.class)))
                .thenThrow(new RuntimeException("MongoDB error"));

            // Should not throw exception
            assertDoesNotThrow(() -> service.publishInvalidation("testMap", "testKey"));
        }
    }

    @Nested
    @DisplayName("Process Invalidations Tests")
    class ProcessInvalidationsTests {
        @Test
        @DisplayName("Should handle eviction errors gracefully")
        void testProcessInvalidationsWithEvictError() throws Exception {
            IMap imap = mock(IMap.class);
            when(hazelcastInstance.getMap("testMap")).thenReturn(imap);
            when(imap.evict(anyString())).thenThrow(new RuntimeException("Evict error"));

            FindIterable<Document> findIterable = mock(FindIterable.class);
            when(mongoCollection.find(any(Document.class))).thenReturn(findIterable);
            when(findIterable.sort(any(Document.class))).thenReturn(findIterable);

            Document invalidationDoc = new Document()
                .append("mapName", "testMap")
                .append("cacheKey", "testKey")
                .append("timestamp", new java.util.Date());

            // Properly mock MongoCursor
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(invalidationDoc);
            when(findIterable.iterator()).thenReturn(cursor);

            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB");

            // Should not throw exception
            assertDoesNotThrow(() -> {
                service.start();
                TimeUnit.SECONDS.sleep(6);
            });
        }
    }
}
