package com.tapdata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
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
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
            service.start();
            assertTrue(service.isRunning());
        }

        @Test
        @DisplayName("Should not start service twice")
        void testStartTwice() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
            service.start();
            service.start(); // Second start should be ignored
            assertTrue(service.isRunning());
        }

        @Test
        @DisplayName("Should stop service successfully")
        void testStop() throws InterruptedException {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
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
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");
            assertDoesNotThrow(() -> service.stop());
            assertFalse(service.isRunning());
        }

    }

    @Nested
    @DisplayName("Publish Invalidation Tests")
    class PublishInvalidationTests {
        @Test
        @DisplayName("Should handle MongoDB exception gracefully")
        void testPublishWithMongoException() {
            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

            when(mongoCollection.insertOne(any(Document.class)))
                .thenThrow(new RuntimeException("MongoDB error"));

            // Should not throw exception
            assertDoesNotThrow(() -> service.publishInvalidation("testMap", "testKey"));
        }
    }

    @Nested
    @DisplayName("Process Invalidations Tests")
    class ProcessInvalidationsTests {

        @SuppressWarnings({"unchecked", "rawtypes"})
        private FindIterable<Document> stubFind(List<Document> firstBatch, List<Document> secondBatch) {
            FindIterable<Document> findIterable = mock(FindIterable.class);
            when(mongoCollection.find()).thenReturn(findIterable);
            when(mongoCollection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.sort(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.limit(anyInt())).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(null);

            final java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);
            doAnswer(inv -> {
                Collection<Document> sink = inv.getArgument(0);
                int n = callCount.getAndIncrement();
                if (n == 0) sink.addAll(firstBatch);
                else if (n == 1) sink.addAll(secondBatch);
                return sink;
            }).when(findIterable).into(any(Collection.class));
            return findIterable;
        }

        @Test
        @DisplayName("Should handle eviction errors gracefully")
        @SuppressWarnings({"unchecked", "rawtypes"})
        void testProcessInvalidationsWithEvictError() throws Exception {
            IMap imap = mock(IMap.class);
            when(hazelcastInstance.getMap("testMap")).thenReturn(imap);
            when(imap.evict(anyString())).thenThrow(new RuntimeException("Evict error"));

            Document invalidationDoc = new Document()
                .append("_id", new ObjectId())
                .append("mapName", "testMap")
                .append("key", "testKey")
                .append("nodeId", "remote-node-" + UUID.randomUUID())
                .append("timestamp", System.currentTimeMillis());

            stubFind(Collections.singletonList(invalidationDoc), Collections.emptyList());

            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", "test-node-id");

            assertDoesNotThrow(() -> {
                service.start();
                TimeUnit.SECONDS.sleep(6);
            });
            verify(imap, atLeastOnce()).evict("testKey");
        }

        @Test
        @DisplayName("Should skip events whose nodeId equals the local node id (self-eviction guard)")
        @SuppressWarnings({"unchecked", "rawtypes"})
        void testSelfEvictionFiltered() throws Exception {
            String localNodeId = "agent-" + UUID.randomUUID();

            IMap imap = mock(IMap.class);
            when(hazelcastInstance.getMap(anyString())).thenReturn(imap);

            Document selfDoc = new Document()
                .append("_id", new ObjectId())
                .append("mapName", "testMap")
                .append("key", "shouldNotEvict")
                .append("nodeId", localNodeId)
                .append("timestamp", System.currentTimeMillis());

            stubFind(Collections.singletonList(selfDoc), Collections.emptyList());

            service = new CacheInvalidationService(hazelcastInstance, mongoClient, "testDB", localNodeId);
            service.start();
            TimeUnit.SECONDS.sleep(6);

            verify(imap, never()).evict(anyString());
        }
    }
}
