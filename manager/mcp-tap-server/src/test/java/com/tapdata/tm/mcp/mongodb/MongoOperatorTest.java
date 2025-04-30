package com.tapdata.tm.mcp.mongodb;

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.CountOptions;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/28 07:31
 */
@ExtendWith(MockitoExtension.class)
class MongoOperatorTest {

    @Mock
    private DataSourceConnectionDto datasourceDto;

    @Mock
    private MongoClient mongoClient;

    @Mock
    private MongoDatabase mongoDatabase;

    @Mock
    private MongoCollection<Document> mongoCollection;

    @Mock
    private FindIterable<Document> findIterable;

    @Mock
    private AggregateIterable<Document> aggregateIterable;

    @Mock
    private MongoCursor<Document> mongoCursor;

    private MongoOperator mongoOperator;

    @BeforeEach
    void setUp() {
        mongoOperator = new MongoOperator(datasourceDto);
        // 注入 mock 的 MongoClient
        try {
            var field = MongoOperator.class.getDeclaredField("mongoClient");
            field.setAccessible(true);
            field.set(mongoOperator, mongoClient);
            
            field = MongoOperator.class.getDeclaredField("database");
            field.setAccessible(true);
            field.set(mongoOperator, "testdb");
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }
    }

    @Test
    void testListCollectionsNameOnly() {
        // 准备测试数据
        List<String> collectionNames = Arrays.asList("collection1", "collection2");
        ListCollectionNamesIterable mongoIterable = mock(ListCollectionNamesIterable.class);
        
        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.listCollectionNames()).thenReturn(mongoIterable);
        doAnswer(invocation -> {
            collectionNames.forEach(name -> ((Consumer<String>)invocation.getArgument(0)).accept(name));
            return null;
        }).when(mongoIterable).forEach(any());

        // 执行测试
        List<?> result = mongoOperator.listCollections(true);

        // 验证结果
        assertEquals(2, result.size());
        assertTrue(result.contains("collection1"));
        assertTrue(result.contains("collection2"));
        verify(mongoDatabase).listCollectionNames();
    }

    @Test
    void testListCollectionsWithFullInfo() {
        // 准备测试数据
        List<Document> collections = Arrays.asList(
            new Document("name", "collection1"),
            new Document("name", "collection2")
        );
        ListCollectionsIterable mongoIterable = mock(ListCollectionsIterable.class);
        
        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.listCollections()).thenReturn(mongoIterable);
        doAnswer(invocation -> {
            collections.forEach(doc -> ((Consumer<Document>)invocation.getArgument(0)).accept(doc));
            return null;
        }).when(mongoIterable).forEach(any());

        // 执行测试
        List<?> result = mongoOperator.listCollections(false);

        // 验证结果
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(doc -> doc instanceof Document));
        verify(mongoDatabase).listCollections();
    }

    @Test
    void testCountWithBasicQuery() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        Document query = new Document("field", "value");
        params.put("query", query);
        
        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.countDocuments(any(Document.class), any(CountOptions.class))).thenReturn(10L);

        // 执行测试
        long count = mongoOperator.count("testCollection", params);

        // 验证结果
        assertEquals(10L, count);
        verify(mongoCollection).countDocuments(any(Document.class), any(CountOptions.class));
    }

    @Test
    void testCountWithComplexOptions() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        params.put("query", new Document("field", "value"));
        params.put("limit", 100);
        params.put("skip", 10);
        params.put("maxTimeMS", 1000L);
        params.put("collation", new Document("locale", "en"));
        params.put("hint", new Document("field", 1));

        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.countDocuments(any(Document.class), any(CountOptions.class))).thenReturn(10L);

        // 执行测试
        long count = mongoOperator.count("testCollection", params);

        // 验证结果
        assertEquals(10L, count);
        verify(mongoCollection).countDocuments(any(Document.class), any(CountOptions.class));
    }

    @Test
    void testQueryWithBasicFilter() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        Document filter = new Document("field", "value");
        params.put("filter", filter);
        List<Document> expectedResults = Collections.singletonList(new Document("_id", 1));

        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.find(any(Document.class))).thenReturn(findIterable);
        when(findIterable.cursor()).thenReturn(mongoCursor);
        doAnswer(answer -> {
            Consumer<Document> consumer = answer.getArgument(0);
            consumer.accept(expectedResults.get(0));
            return null;
        }).when(mongoCursor).forEachRemaining(any());

        // 执行测试
        List<Document> results = mongoOperator.query("testCollection", params);

        // 验证结果
        assertEquals(1, results.size());
        assertEquals(expectedResults.get(0), results.get(0));
        verify(findIterable).limit(100);
    }

    @Test
    void testQueryWithExplain() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        params.put("filter", new Document("field", "value"));
        params.put("explain", "executionStats");
        Document explainResult = new Document("executionStats", new Document("nReturned", 1));

        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.find(any(Document.class))).thenReturn(findIterable);
        when(findIterable.explain(any(ExplainVerbosity.class))).thenReturn(explainResult);

        // 执行测试
        List<Document> results = mongoOperator.query("testCollection", params);

        // 验证结果
        assertEquals(1, results.size());
        assertEquals(explainResult, results.get(0));
        verify(findIterable).explain(ExplainVerbosity.EXECUTION_STATS);
    }

    @Test
    void testAggregateWithBasicPipeline() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        List<Document> pipeline = Collections.singletonList(
            new Document("$match", new Document("field", "value"))
        );
        params.put("pipeline", pipeline);
        Document aggregateResult = new Document("result", "value");

        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        when(aggregateIterable.cursor()).thenReturn(mongoCursor);
        doAnswer(answer -> {
            ((Consumer<Document>) answer.getArgument(0)).accept(aggregateResult);
            return null;
        }).when(mongoCursor).forEachRemaining(any());

        // 执行测试
        List<Document> results = mongoOperator.aggregate("testCollection", params);

        // 验证结果
        assertEquals(1, results.size());
        assertEquals(aggregateResult, results.get(0));
        verify(mongoCollection).aggregate(anyList());
    }

    @Test
    void testAggregateWithExplain() {
        // 准备测试数据
        Map<String, Object> params = new HashMap<>();
        List<Document> pipeline = Collections.singletonList(
            new Document("$match", new Document("field", "value"))
        );
        params.put("pipeline", pipeline);
        params.put("explain", "queryPlanner");
        Document explainResult = new Document("queryPlanner", new Document("winningPlan", "plan"));

        // 设置 mock 行为
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        when(aggregateIterable.explain(any(ExplainVerbosity.class))).thenReturn(explainResult);

        // 执行测试
        List<Document> results = mongoOperator.aggregate("testCollection", params);

        // 验证结果
        assertEquals(1, results.size());
        assertEquals(explainResult, results.get(0));
        verify(aggregateIterable).explain(ExplainVerbosity.QUERY_PLANNER);
    }

    @Test
    void testConnect() {
        // 准备测试数据
        Map<String, Object> config = new HashMap<>();
        config.put("isUri", true);
        config.put("uri", "mongodb://localhost:27017/test");
        
        // 设置 mock 行为
        when(datasourceDto.getConfig()).thenReturn(config);
        
        try (MockedStatic<MongoClients> mongoClientsMock = mockStatic(MongoClients.class)) {
            mongoClientsMock.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                .thenReturn(mongoClient);

            // 执行测试
            mongoOperator.connect();

            // 验证结果
            mongoClientsMock.verify(() -> MongoClients.create(any(MongoClientSettings.class)));
        }
    }

    @Test
    void testConnectWithSSL() {
        // 准备测试数据
        Map<String, Object> config = new HashMap<>();
        config.put("isUri", true);
        config.put("uri", "mongodb://localhost:27017/test?ssl=true&tlsAllowInvalidCertificates=true");
        config.put("ssl", true);
        
        // 设置 mock 行为
        when(datasourceDto.getConfig()).thenReturn(config);
        
        try (MockedStatic<MongoClients> mongoClientsMock = mockStatic(MongoClients.class)) {
            mongoClientsMock.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                .thenReturn(mongoClient);

            // 执行测试
            mongoOperator.connect();

            // 验证结果
            mongoClientsMock.verify(() -> MongoClients.create(any(MongoClientSettings.class)));
        }
    }

    @Test
    void testConnectWithCredentials() {
        // 准备测试数据
        Map<String, Object> config = new HashMap<>();
        config.put("isUri", false);
        config.put("host", "localhost:27017");
        config.put("database", "test");
        config.put("user", "testuser");
        config.put("password", "testpass");
        config.put("additionalString", "authSource=admin");
        
        // 设置 mock 行为
        when(datasourceDto.getConfig()).thenReturn(config);
        
        try (MockedStatic<MongoClients> mongoClientsMock = mockStatic(MongoClients.class)) {
            mongoClientsMock.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                .thenReturn(mongoClient);

            // 执行测试
            mongoOperator.connect();

            // 验证结果
            mongoClientsMock.verify(() -> MongoClients.create(any(MongoClientSettings.class)));
        }
    }

    @Test
    void testClose() throws Exception {
        // 执行测试
        mongoOperator.close();

        // 验证结果
        verify(mongoClient).close();
    }
}
