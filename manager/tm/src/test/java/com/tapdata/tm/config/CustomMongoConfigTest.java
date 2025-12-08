package com.tapdata.tm.config;

import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;
import com.tapdata.tm.base.entity.BaseEntity;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for CustomMongoConfig
 * 
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @version v1.0 2025/9/28 Create
 */
class CustomMongoConfigTest {
    CustomMongoConfig customMongoConfig;
    MongoTemplate mongoTemplate;
    MongoDatabase mongoDatabase;
    MongoCollection<Document> mongoCollection;
    MongoCollection<Document> backupCollection;
    MongoCursor<Document> mongoCursor;
    
    @BeforeEach
    void init() {
        mongoTemplate = mock(MongoTemplate.class);
        mongoDatabase = mock(MongoDatabase.class);
        mongoCollection = mock(MongoCollection.class);
        backupCollection = mock(MongoCollection.class);
        mongoCursor = mock(MongoCursor.class);
        customMongoConfig = spy(new CustomMongoConfig(mongoTemplate));
        ListIndexesIterable<Document> indexesIterable = new ListIndexesIterable<Document>() {
            @Override
            public ListIndexesIterable<Document> maxTime(long l, TimeUnit timeUnit) {
                return this;
            }

            @Override
            public ListIndexesIterable<Document> batchSize(int i) {
                return this;
            }

            @Override
            public ListIndexesIterable<Document> comment(String s) {
                return this;
            }

            @Override
            public ListIndexesIterable<Document> comment(BsonValue bsonValue) {
                return this;
            }

            @Override
            public ListIndexesIterable<Document> timeoutMode(TimeoutMode timeoutMode) {
                return this;
            }

            @Override
            public MongoCursor<Document> iterator() {
                return null;
            }

            @Override
            public MongoCursor<Document> cursor() {
                return null;
            }

            @Override
            public Document first() {
                return null;
            }

            @Override
            public <U> MongoIterable<U> map(Function<Document, U> function) {
                return null;
            }

            @Override
            public <A extends Collection<? super Document>> A into(A objects) {
                return null;
            }
        };
        when(backupCollection.listIndexes()).thenReturn(indexesIterable);
    }

    @Nested
    class initTest {
        @Test
        void testNormal() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).init();
            doNothing().when(temp).eachOnce(any(BeanDefinition.class));
            Assertions.assertDoesNotThrow(temp::init);
        }
    }

    @Nested
    class eachOnceTest {

        @org.springframework.data.mongodb.core.mapping.Document("A")
        class A extends BaseEntity {

        }

        @org.springframework.data.mongodb.core.mapping.Document("B")
        @CappedCollection
        class B extends BaseEntity {

        }

        @org.springframework.data.mongodb.core.mapping.Document("B")
        @CappedCollection(capped = false)
        class B1 extends BaseEntity {

        }

        class C extends BaseEntity {

        }

        @Test
        void testNormal() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).eachOnce(any(BeanDefinition.class));
            doNothing().when(temp).handleCappedCollection(anyString(), any());
            BeanDefinition mock = mock(BeanDefinition.class);
            when(mock.getBeanClassName()).thenReturn(B.class.getName());
            Assertions.assertDoesNotThrow(() -> temp.eachOnce(mock));
            verify(temp, times(1)).handleCappedCollection(anyString(), any());
        }

        @Test
        void testNotCappedCollection() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).eachOnce(any(BeanDefinition.class));
            doNothing().when(temp).handleCappedCollection(anyString(), any());
            BeanDefinition mock = mock(BeanDefinition.class);
            when(mock.getBeanClassName()).thenReturn(A.class.getName());
            Assertions.assertDoesNotThrow(() -> temp.eachOnce(mock));
            verify(temp, times(0)).handleCappedCollection(anyString(), any());
        }

        @Test
        void testNotCappedCollection2() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).eachOnce(any(BeanDefinition.class));
            doNothing().when(temp).handleCappedCollection(anyString(), any());
            BeanDefinition mock = mock(BeanDefinition.class);
            when(mock.getBeanClassName()).thenReturn(B1.class.getName());
            Assertions.assertDoesNotThrow(() -> temp.eachOnce(mock));
            verify(temp, times(0)).handleCappedCollection(anyString(), any());
        }

        @Test
        void testNotCappedCollectionAndDocument() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).eachOnce(any(BeanDefinition.class));
            doNothing().when(temp).handleCappedCollection(anyString(), any());
            BeanDefinition mock = mock(BeanDefinition.class);
            when(mock.getBeanClassName()).thenReturn(C.class.getName());
            Assertions.assertDoesNotThrow(() -> temp.eachOnce(mock));
            verify(temp, times(0)).handleCappedCollection(anyString(), any());
        }

        @Test
        void testException() {
            CustomMongoConfig temp = mock(CustomMongoConfig.class);
            doCallRealMethod().when(temp).eachOnce(any(BeanDefinition.class));
            doAnswer(a -> {throw new RuntimeException("test");}).when(temp).handleCappedCollection(anyString(), any());
            BeanDefinition mock = mock(BeanDefinition.class);
            when(mock.getBeanClassName()).thenReturn(B.class.getName());
            Assertions.assertDoesNotThrow(() -> temp.eachOnce(mock));
            verify(temp, times(1)).handleCappedCollection(anyString(), any());
        }
    }
    
    @Nested
    class constructorTest {
        
        @Test
        void testNormal() {
            MongoTemplate template = mock(MongoTemplate.class);
            
            Assertions.assertDoesNotThrow(() -> {
                CustomMongoConfig config = new CustomMongoConfig(template);
                Assertions.assertNotNull(config);
            });
        }
        
        @Test
        void testNullTemplate() {
            Assertions.assertDoesNotThrow(() -> {
                CustomMongoConfig config = new CustomMongoConfig(null);
                Assertions.assertNotNull(config);
            });
        }
    }
    
    @Nested
    class batchTest {
        
        @Test
        void testDefaultBatchSize() {
            int result = ReflectionTestUtils.invokeMethod(customMongoConfig, "batch");
            
            Assertions.assertEquals(2000, result);
        }
        
        @Test
        void testCustomBatchSize() {
            ReflectionTestUtils.setField(customMongoConfig, "batchSize", 5000);
            
            int result = ReflectionTestUtils.invokeMethod(customMongoConfig, "batch");
            
            Assertions.assertEquals(5000, result);
        }
    }
    
    @Nested
    class handleCappedCollectionTest {
        
        @Test
        void testCollectionNotExists() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            when(mongoTemplate.collectionExists("testCollection")).thenReturn(false);
            doCallRealMethod().when(customMongoConfig).handleCappedCollection("testCollection", annotation);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleCappedCollection("testCollection", annotation));
            verify(customMongoConfig, times(1)).createCappedCollection("testCollection", annotation);
            verify(customMongoConfig, never()).updateExistingCollection(anyString(), any());
        }
        
        @Test
        void testCollectionExists() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            when(mongoTemplate.collectionExists("testCollection")).thenReturn(true);
            doCallRealMethod().when(customMongoConfig).handleCappedCollection("testCollection", annotation);
            doNothing().when(customMongoConfig).updateExistingCollection(anyString(), any());
            
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleCappedCollection("testCollection", annotation));
            verify(customMongoConfig, never()).createCappedCollection(anyString(), any());
            verify(customMongoConfig, times(1)).updateExistingCollection("testCollection", annotation);
        }
        
        @Test
        void testException() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            when(mongoTemplate.collectionExists("testCollection")).thenThrow(new RuntimeException("Database error"));
            doCallRealMethod().when(customMongoConfig).handleCappedCollection("testCollection", annotation);
            
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleCappedCollection("testCollection", annotation));
            verify(customMongoConfig, never()).createCappedCollection(anyString(), any());
            verify(customMongoConfig, never()).updateExistingCollection(anyString(), any());
        }
    }

    @Nested
    class createCappedCollectionTest {

        @Test
        void testNormal() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            doCallRealMethod().when(customMongoConfig).createCappedCollection("testCollection", annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.createCappedCollection("testCollection", annotation));
            verify(mongoTemplate, times(1)).createCollection(eq("testCollection"), any(CollectionOptions.class));
        }

        @Test
        void testZeroLength() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 0L, 1024L * 1024L);
            doCallRealMethod().when(customMongoConfig).createCappedCollection("testCollection", annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.createCappedCollection("testCollection", annotation));
            verify(mongoTemplate, times(1)).createCollection(eq("testCollection"), any(CollectionOptions.class));
        }

        @Test
        void testNegativeLength() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, -100L, 1024L * 1024L);
            doCallRealMethod().when(customMongoConfig).createCappedCollection("testCollection", annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.createCappedCollection("testCollection", annotation));
            verify(mongoTemplate, times(1)).createCollection(eq("testCollection"), any(CollectionOptions.class));
        }

        @Test
        void testZeroSize() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 0L);
            doCallRealMethod().when(customMongoConfig).createCappedCollection("testCollection", annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.createCappedCollection("testCollection", annotation));
            verify(mongoTemplate, times(1)).createCollection(eq("testCollection"), any(CollectionOptions.class));
        }

        @Test
        void testNegativeSize() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, -1024L);
            doCallRealMethod().when(customMongoConfig).createCappedCollection("testCollection", annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.createCappedCollection("testCollection", annotation));
            verify(mongoTemplate, times(1)).createCollection(eq("testCollection"), any(CollectionOptions.class));
        }
    }

    @Nested
    class updateExistingCollectionTest {

        @Test
        void testCappedCollectionUpToDate() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L * 1024L, 1000L);

            doCallRealMethod().when(customMongoConfig).updateExistingCollection("testCollection", annotation);
            doReturn(stats).when(customMongoConfig).getCollectionStats("testCollection");
            doReturn(false).when(customMongoConfig).needsUpdate(stats, annotation);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.updateExistingCollection("testCollection", annotation));
            verify(customMongoConfig, never()).backupAndRecreateCollection(anyString(), any());
        }

        @Test
        void testException() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);

            doCallRealMethod().when(customMongoConfig).updateExistingCollection("testCollection", annotation);
            doThrow(new RuntimeException("Database error")).when(customMongoConfig).getCollectionStats("testCollection");

            Assertions.assertDoesNotThrow(() -> customMongoConfig.updateExistingCollection("testCollection", annotation));
            verify(customMongoConfig, never()).backupAndRecreateCollection(anyString(), any());
        }
    }

    @Nested
    class getCollectionStatsTest {

        @Test
        void testNormal() {
            Document collStats = new Document();
            collStats.put("capped", true);
            collStats.put("maxSize", "1048576");
            collStats.put("max", "1000");

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenReturn(collStats);
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");

            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isCapped());
            Assertions.assertEquals(1048576L, result.getMaxSize());
            Assertions.assertEquals(1000L, result.getMax());
        }

        @Test
        void testNotCapped() {
            Document collStats = new Document();
            collStats.put("capped", false);

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenReturn(collStats);
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");

            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");

            Assertions.assertNotNull(result);
            Assertions.assertFalse(result.isCapped());
            Assertions.assertEquals(-1L, result.getMaxSize());
            Assertions.assertEquals(0L, result.getMax());
        }

        @Test
        void testMissingFields() {
            Document collStats = new Document();
            collStats.put("capped", true);
            // maxSize and max are missing

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenReturn(collStats);
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");

            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isCapped());
            Assertions.assertEquals(-1L, result.getMaxSize());
            Assertions.assertEquals(0L, result.getMax());
        }

        @Test
        void testException() {
            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenThrow(new RuntimeException("Database error"));
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");

            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");

            Assertions.assertNotNull(result);
            Assertions.assertFalse(result.isCapped());
            Assertions.assertEquals(0L, result.getMaxSize());
            Assertions.assertEquals(0L, result.getMax());
        }
    }

    @Nested
    class needsUpdateTest {

        @Test
        void testWithMaxMemoryNeedsUpdate() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 512L * 1024L, 500L);

            doCallRealMethod().when(customMongoConfig).needsUpdate(stats, annotation);

            boolean result = customMongoConfig.needsUpdate(stats, annotation);

            Assertions.assertTrue(result);
        }

        @Test
        void testWithMaxMemoryUpToDate() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L * 1024L, 1000L);

            doCallRealMethod().when(customMongoConfig).needsUpdate(stats, annotation);

            boolean result = customMongoConfig.needsUpdate(stats, annotation);

            Assertions.assertFalse(result);
        }

        @Test
        void testWithoutMaxMemoryNeedsUpdate() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 0L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L * 1024L, 500L);

            doCallRealMethod().when(customMongoConfig).needsUpdate(stats, annotation);

            boolean result = customMongoConfig.needsUpdate(stats, annotation);

            Assertions.assertTrue(result);
        }

        @Test
        void testWithoutMaxMemoryUpToDate() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 0L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L * 1024L, 1000L);

            doCallRealMethod().when(customMongoConfig).needsUpdate(stats, annotation);

            boolean result = customMongoConfig.needsUpdate(stats, annotation);

            Assertions.assertFalse(result);
        }

        @Test
        void testNegativeMaxMemory() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, -1L);
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L * 1024L, 500L);

            doCallRealMethod().when(customMongoConfig).needsUpdate(stats, annotation);

            boolean result = customMongoConfig.needsUpdate(stats, annotation);

            Assertions.assertTrue(result);
        }
    }

    @Nested
    class recreateCappedCollectionTest {

        @Test
        void testNormal() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            when(mongoDatabase.getCollection(startsWith("testCollection_backup_"))).thenReturn(backupCollection);
            when(mongoDatabase.getName()).thenReturn("testdb");
            when(backupCollection.countDocuments()).thenReturn(500L);
            when(mongoCollection.countDocuments()).thenReturn(500L);
            when(backupCollection.find()).thenReturn(mock(com.mongodb.client.FindIterable.class));
            when(backupCollection.find().iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(false);
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            doNothing().when(mongoCollection).renameCollection(any(MongoNamespace.class));

            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(mongoCollection, times(1)).renameCollection(any(MongoNamespace.class));
            verify(customMongoConfig, times(0)).createCappedCollection("testCollection", annotation);
            verify(backupCollection, times(0)).drop();
        }

        @Test
        void testWithDataMigration() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            ReflectionTestUtils.setField(customMongoConfig, "batchSize", 2);

            List<Document> documents = Arrays.asList(
                new Document("_id", "1").append("data", "test1"),
                new Document("_id", "2").append("data", "test2"),
                new Document("_id", "3").append("data", "test3")
            );

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            when(mongoDatabase.getCollection(startsWith("testCollection_backup_"))).thenReturn(backupCollection);
            when(mongoDatabase.getName()).thenReturn("testdb");
            when(backupCollection.countDocuments()).thenReturn(3L);
            when(mongoCollection.countDocuments()).thenReturn(3L);
            when(backupCollection.find()).thenReturn(mock(com.mongodb.client.FindIterable.class));
            when(backupCollection.find().iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(true, true, true, false);
            when(mongoCursor.next()).thenReturn(documents.get(0), documents.get(1), documents.get(2));
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            doNothing().when(mongoCollection).renameCollection(any(MongoNamespace.class));
            when(mongoCollection.insertMany(any(List.class), any(InsertManyOptions.class))).thenReturn(mock(InsertManyResult.class));
            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(mongoCollection, times(2)).insertMany(any(List.class), any(InsertManyOptions.class));
            verify(backupCollection, times(0)).drop();
        }

        @Test
        void testWithMongoExceptionDuringInsert() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            ReflectionTestUtils.setField(customMongoConfig, "batchSize", 2);

            List<Document> documents = Arrays.asList(
                new Document("_id", "1").append("data", "test1"),
                new Document("_id", "2").append("data", "test2")
            );

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            when(mongoDatabase.getCollection(startsWith("testCollection_backup_"))).thenReturn(backupCollection);
            when(mongoDatabase.getName()).thenReturn("testdb");
            when(backupCollection.countDocuments()).thenReturn(2L);
            when(mongoCollection.countDocuments()).thenReturn(0L);
            when(backupCollection.find()).thenReturn(mock(com.mongodb.client.FindIterable.class));
            when(backupCollection.find().iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(true, true, false);
            when(mongoCursor.next()).thenReturn(documents.get(0), documents.get(1));
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            doNothing().when(mongoCollection).renameCollection(any(MongoNamespace.class));
            doThrow(new MongoException("Capped collection full")).when(mongoCollection).insertMany(any(List.class), any(InsertManyOptions.class));

            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(mongoCollection, times(2)).insertMany(any(List.class), any(InsertManyOptions.class));
            verify(backupCollection, never()).drop(); // Should not drop backup when migration fails
        }

        @Test
        void testWithFinalBatchException() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);
            ReflectionTestUtils.setField(customMongoConfig, "batchSize", 3);

            List<Document> documents = Arrays.asList(
                new Document("_id", "1").append("data", "test1"),
                new Document("_id", "2").append("data", "test2")
            );

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            when(mongoDatabase.getCollection(startsWith("testCollection_backup_"))).thenReturn(backupCollection);
            when(mongoDatabase.getName()).thenReturn("testdb");
            when(backupCollection.countDocuments()).thenReturn(2L);
            when(mongoCollection.countDocuments()).thenReturn(0L);
            when(backupCollection.find()).thenReturn(mock(com.mongodb.client.FindIterable.class));
            when(backupCollection.find().iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(true, true, false);
            when(mongoCursor.next()).thenReturn(documents.get(0), documents.get(1));
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            doNothing().when(mongoCollection).renameCollection(any(MongoNamespace.class));
            doThrow(new MongoException("Capped collection full")).when(mongoCollection).insertMany(any(List.class), any(InsertManyOptions.class));

            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(mongoCollection, times(1)).insertMany(any(List.class), any(InsertManyOptions.class));
            verify(backupCollection, never()).drop();
        }

        @Test
        void testEmptyBackupCollection() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            when(mongoDatabase.getCollection(startsWith("testCollection_backup_"))).thenReturn(backupCollection);
            when(mongoDatabase.getName()).thenReturn("testdb");
            when(backupCollection.countDocuments()).thenReturn(0L);
            when(mongoCollection.countDocuments()).thenReturn(0L);
            when(backupCollection.find()).thenReturn(mock(com.mongodb.client.FindIterable.class));
            when(backupCollection.find().iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(false);
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);
            doNothing().when(customMongoConfig).createCappedCollection(anyString(), any());
            doNothing().when(mongoCollection).renameCollection(any(MongoNamespace.class));

            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(backupCollection, times(0)).drop();
        }

        @Test
        void testException() {
            CappedCollection annotation = createCappedCollectionAnnotation(true, 1000L, 1024L * 1024L);

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
            doThrow(new RuntimeException("Database error")).when(mongoCollection).renameCollection(any(MongoNamespace.class));
            Runnable r = () -> {};
            doCallRealMethod().when(customMongoConfig).backupAndRecreateCollection("testCollection", r);

            Assertions.assertDoesNotThrow(() -> customMongoConfig.backupAndRecreateCollection("testCollection", r));
            verify(customMongoConfig, never()).createCappedCollection(anyString(), any());
        }
    }

    @Nested
    class collectionStatsTest {

        @Test
        void testConstructor() {
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, 1024L, 100L);

            Assertions.assertTrue(stats.isCapped());
            Assertions.assertEquals(1024L, stats.getMaxSize());
            Assertions.assertEquals(100L, stats.getMax());
        }

        @Test
        void testConstructorWithFalse() {
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(false, 0L, 0L);

            Assertions.assertFalse(stats.isCapped());
            Assertions.assertEquals(0L, stats.getMaxSize());
            Assertions.assertEquals(0L, stats.getMax());
        }

        @Test
        void testConstructorWithNegativeValues() {
            CustomMongoConfig.CollectionStats stats = new CustomMongoConfig.CollectionStats(true, -1L, -1L);

            Assertions.assertTrue(stats.isCapped());
            Assertions.assertEquals(-1L, stats.getMaxSize());
            Assertions.assertEquals(-1L, stats.getMax());
        }
    }

    @Nested
    class edgeCasesTest {
        @Test
        void testException() {
            when(mongoTemplate.getDb()).thenAnswer(a -> {throw new IOException("Error");});
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");
            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");
            Assertions.assertNotNull(result);
            Assertions.assertFalse(result.isCapped());
            Assertions.assertEquals(0L, result.getMaxSize());
            Assertions.assertEquals(0L, result.getMax());
        }

        @Test
        void testGetCollectionStatsWithNullValues() {
            Document collStats = new Document();
            collStats.put("capped", null);
            collStats.put("maxSize", null);
            collStats.put("max", null);

            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenReturn(collStats);
            doCallRealMethod().when(customMongoConfig).getCollectionStats("testCollection");

            CustomMongoConfig.CollectionStats result = customMongoConfig.getCollectionStats("testCollection");

            Assertions.assertNotNull(result);
            Assertions.assertFalse(result.isCapped());
            Assertions.assertEquals(-1L, result.getMaxSize());
            Assertions.assertEquals(0L, result.getMax());
        }
    }

    @Nested
    class insertManyTest {
        @Test
        void testInsertMany() {
            List<Document> documents = Arrays.asList(
                new Document("_id", "1").append("data", "test1"),
                new Document("_id", "2").append("data", "test2")
            );

            when(mongoCollection.insertMany(any(List.class), any(InsertManyOptions.class))).thenAnswer(a -> {throw new MongoException("Capped collection full");});
            doCallRealMethod().when(customMongoConfig).insertMany(any(), any());

            Assertions.assertFalse(customMongoConfig.insertMany(mongoCollection, documents));
        }
    }

    @Nested
    class handleNonCappedCollectionTest {
        @Test
        void testNormal() {
            CustomMongoConfig.CollectionStats stats = mock(CustomMongoConfig.CollectionStats.class);
            when(mongoTemplate.collectionExists("collectionName")).thenReturn(false);
            when(customMongoConfig.getCollectionStats("collectionName")).thenReturn(stats);
            when(stats.isCapped()).thenReturn(false);
            doNothing().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            doCallRealMethod().when(customMongoConfig).handleNonCappedCollection("collectionName");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleNonCappedCollection("collectionName"));
            verify(mongoTemplate, times(1)).collectionExists("collectionName");
            verify(customMongoConfig, times(0)).getCollectionStats("collectionName");
            verify(customMongoConfig, times(0)).changeToNonCappedCollection("collectionName");
            verify(stats, times(0)).isCapped();
        }
        @Test
        void testCollectionExists() {
            CustomMongoConfig.CollectionStats stats = mock(CustomMongoConfig.CollectionStats.class);
            when(mongoTemplate.collectionExists("collectionName")).thenReturn(true);
            when(customMongoConfig.getCollectionStats("collectionName")).thenReturn(stats);
            when(stats.isCapped()).thenReturn(false);
            doNothing().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            doCallRealMethod().when(customMongoConfig).handleNonCappedCollection("collectionName");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleNonCappedCollection("collectionName"));
            verify(mongoTemplate, times(1)).collectionExists("collectionName");
            verify(customMongoConfig, times(1)).getCollectionStats("collectionName");
            verify(customMongoConfig, times(0)).changeToNonCappedCollection("collectionName");
            verify(stats, times(1)).isCapped();
        }
        @Test
        void testIsCapped() {
            CustomMongoConfig.CollectionStats stats = mock(CustomMongoConfig.CollectionStats.class);
            when(mongoTemplate.collectionExists("collectionName")).thenReturn(true);
            when(customMongoConfig.getCollectionStats("collectionName")).thenReturn(stats);
            when(stats.isCapped()).thenReturn(true);
            doNothing().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            doCallRealMethod().when(customMongoConfig).handleNonCappedCollection("collectionName");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleNonCappedCollection("collectionName"));
            verify(mongoTemplate, times(1)).collectionExists("collectionName");
            verify(customMongoConfig, times(1)).getCollectionStats("collectionName");
            verify(customMongoConfig, times(1)).changeToNonCappedCollection("collectionName");
            verify(stats, times(1)).isCapped();
        }
        @Test
        void testNotIsCapped() {
            CustomMongoConfig.CollectionStats stats = mock(CustomMongoConfig.CollectionStats.class);
            when(mongoTemplate.collectionExists("collectionName")).thenReturn(true);
            when(customMongoConfig.getCollectionStats("collectionName")).thenReturn(stats);
            when(stats.isCapped()).thenReturn(false);
            doNothing().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            doCallRealMethod().when(customMongoConfig).handleNonCappedCollection("collectionName");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleNonCappedCollection("collectionName"));
            verify(mongoTemplate, times(1)).collectionExists("collectionName");
            verify(customMongoConfig, times(1)).getCollectionStats("collectionName");
            verify(customMongoConfig, times(0)).changeToNonCappedCollection("collectionName");
            verify(stats, times(1)).isCapped();
        }
        @Test
        void testException() {
            CustomMongoConfig.CollectionStats stats = mock(CustomMongoConfig.CollectionStats.class);
            when(mongoTemplate.collectionExists("collectionName")).thenAnswer(a -> {throw new IOException("EXCEPTION");});
            when(customMongoConfig.getCollectionStats("collectionName")).thenReturn(stats);
            when(stats.isCapped()).thenReturn(false);
            doNothing().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            doCallRealMethod().when(customMongoConfig).handleNonCappedCollection("collectionName");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.handleNonCappedCollection("collectionName"));
            verify(mongoTemplate, times(1)).collectionExists("collectionName");
            verify(customMongoConfig, times(0)).getCollectionStats("collectionName");
            verify(customMongoConfig, times(0)).changeToNonCappedCollection("collectionName");
            verify(stats, times(0)).isCapped();
        }
    }

    @Nested
    class changeToNonCappedCollectionTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            when(mongoTemplate.createCollection("collectionName")).thenReturn(mock(MongoCollection.class));
            doAnswer(a -> {
                Runnable argument = (Runnable) a.getArgument(1);
                argument.run();
                return null;
            }).when(customMongoConfig).backupAndRecreateCollection(anyString(), any(Runnable.class));
            Assertions.assertDoesNotThrow(() -> customMongoConfig.changeToNonCappedCollection("collectionName"));
            verify(customMongoConfig, times(1)).backupAndRecreateCollection(anyString(), any(Runnable.class));
        }
        @Test
        void testException() {
            doCallRealMethod().when(customMongoConfig).changeToNonCappedCollection("collectionName");
            when(mongoTemplate.createCollection("collectionName")).thenReturn(mock(MongoCollection.class));
            doAnswer(a -> {
                throw new IOException("E");
            }).when(customMongoConfig).backupAndRecreateCollection(anyString(), any(Runnable.class));
            Assertions.assertDoesNotThrow(() -> customMongoConfig.changeToNonCappedCollection("collectionName"));
            verify(customMongoConfig, times(1)).backupAndRecreateCollection(anyString(), any(Runnable.class));
        }
    }

    @Nested
    class doAfterTest {
        MongoCollection<org.bson.Document> newCollection;
        MongoCollection<org.bson.Document> backupCollection;
        ListIndexesIterable<org.bson.Document> indexesIterable;
        @BeforeEach
        void init() {
            newCollection = mock(MongoCollection.class);
            backupCollection = mock(MongoCollection.class);
            MongoCursor<Document> cursor = mongoCursor;
            indexesIterable = new ListIndexesIterable<Document>() {
                @Override
                public ListIndexesIterable<Document> maxTime(long l, TimeUnit timeUnit) {
                    return this;
                }

                @Override
                public ListIndexesIterable<Document> batchSize(int i) {
                    return this;
                }

                @Override
                public ListIndexesIterable<Document> comment(String s) {
                    return this;
                }

                @Override
                public ListIndexesIterable<Document> comment(BsonValue bsonValue) {
                    return this;
                }

                @Override
                public ListIndexesIterable<Document> timeoutMode(TimeoutMode timeoutMode) {
                    return this;
                }

                @Override
                public MongoCursor<Document> iterator() {
                    return cursor;
                }

                @Override
                public MongoCursor<Document> cursor() {
                    return cursor;
                }

                @Override
                public Document first() {
                    return new Document();
                }

                @Override
                public <U> MongoIterable<U> map(Function<Document, U> function) {
                    return null;
                }

                @Override
                public <A extends Collection<? super Document>> A into(A objects) {
                    return null;
                }
            };
        }
        @Test
        void testNormal() {
            when(newCollection.countDocuments()).thenReturn(100L);
            when(backupCollection.listIndexes()).thenReturn(indexesIterable);
            when(backupCollection.countDocuments()).thenReturn(100L);
            doCallRealMethod().when(customMongoConfig).doAfter(newCollection, backupCollection, 100L);
            doNothing().when(customMongoConfig).syncIndex(anyList(), any());
            Assertions.assertDoesNotThrow(() -> customMongoConfig.doAfter(newCollection, backupCollection, 100L));
        }
        @Test
        void test1() {
            when(newCollection.countDocuments()).thenReturn(1L);
            when(backupCollection.listIndexes()).thenReturn(indexesIterable);
            when(backupCollection.countDocuments()).thenReturn(100L);
            doCallRealMethod().when(customMongoConfig).doAfter(newCollection, backupCollection, 100L);
            doNothing().when(customMongoConfig).syncIndex(anyList(), any());
            Assertions.assertDoesNotThrow(() -> customMongoConfig.doAfter(newCollection, backupCollection, 100L));
        }
        @Test
        void test2() {
            when(newCollection.countDocuments()).thenReturn(1L);
            when(backupCollection.listIndexes()).thenReturn(indexesIterable);
            when(backupCollection.countDocuments()).thenReturn(0L);
            doCallRealMethod().when(customMongoConfig).doAfter(newCollection, backupCollection, 100L);
            doNothing().when(customMongoConfig).syncIndex(anyList(), any());
            Assertions.assertDoesNotThrow(() -> customMongoConfig.doAfter(newCollection, backupCollection, 100L));
        }
    }

    @Nested
    class syncIndexTest {
        @Test
        void testNormal() {
            List<org.bson.Document> indexes = new ArrayList<>();
            MongoCollection<org.bson.Document> newCollection = mock(MongoCollection.class);
            doCallRealMethod().when(customMongoConfig).syncIndex(anyList(), any());
            when(newCollection.createIndex(any(org.bson.Document.class))).thenReturn("");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.syncIndex(indexes, newCollection));
            verify(newCollection, times(0)).createIndex(any(org.bson.Document.class));
        }
        @Test
        void test1() {
            List<org.bson.Document> indexes = new ArrayList<>();
            Document d = new Document();
            indexes.add(d);
            d.put("key", new Document());
            MongoCollection<org.bson.Document> newCollection = mock(MongoCollection.class);
            doCallRealMethod().when(customMongoConfig).syncIndex(anyList(), any());
            when(newCollection.createIndex(any(org.bson.Document.class))).thenReturn("");
            Assertions.assertDoesNotThrow(() -> customMongoConfig.syncIndex(indexes, newCollection));
            verify(newCollection, times(1)).createIndex(any(org.bson.Document.class));
        }
        @Test
        void testException() {
            List<org.bson.Document> indexes = new ArrayList<>();
            Document d = new Document();
            indexes.add(d);
            d.put("key", new Document());
            MongoCollection<org.bson.Document> newCollection = mock(MongoCollection.class);
            doCallRealMethod().when(customMongoConfig).syncIndex(anyList(), any());
            when(newCollection.createIndex(any(org.bson.Document.class))).thenAnswer(a -> {throw new RuntimeException("test");});
            Assertions.assertDoesNotThrow(() -> customMongoConfig.syncIndex(indexes, newCollection));
            verify(newCollection, times(1)).createIndex(any(org.bson.Document.class));
        }
    }

    // Helper method
    private CappedCollection createCappedCollectionAnnotation(boolean capped, long maxLength, long maxMemory) {
        CappedCollection annotation = mock(CappedCollection.class);
        when(annotation.capped()).thenReturn(capped);
        when(annotation.maxLength()).thenReturn(maxLength);
        when(annotation.maxMemory()).thenReturn(maxMemory);
        return annotation;
    }
}
