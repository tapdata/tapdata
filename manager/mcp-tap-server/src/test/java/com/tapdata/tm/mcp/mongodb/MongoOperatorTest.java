package com.tapdata.tm.mcp.mongodb;

import com.mongodb.Function;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.*;
import com.mongodb.client.internal.MongoIterableImpl;
import com.mongodb.client.model.CountOptions;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/28 07:31
 */
class MongoOperatorTest {

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

    @Mock
    private MongoIterable<String> mongoIterable;

    private MongoOperator mongoOperator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        DataSourceConnectionDto datasourceDto = new DataSourceConnectionDto();
        mongoOperator = new MongoOperator(datasourceDto);
        ReflectionTestUtils.setField(mongoOperator, "mongoClient", mongoClient);
        ReflectionTestUtils.setField(mongoOperator, "database", "testDB");
    }

    @Test
    void testListCollections() {
        List<String> data = Arrays.asList("collection1", "collection2");
        AtomicInteger i = new AtomicInteger(0);

        when(mongoDatabase.listCollectionNames()).thenReturn(new MongoIterable<String>() {
            @Override
            public MongoCursor<String> iterator() {
                return new MongoCursor<String>() {

                    @Override
                    public void close() {

                    }

                    @Override
                    public boolean hasNext() {
                        return i.get() < data.size();
                    }

                    @Override
                    public String next() {
                        return data.get(i.getAndIncrement());
                    }

                    @Override
                    public int available() {
                        return 0;
                    }

                    @Override
                    public String tryNext() {
                        return "";
                    }

                    @Override
                    public ServerCursor getServerCursor() {
                        return null;
                    }

                    @Override
                    public ServerAddress getServerAddress() {
                        return null;
                    }
                };
            }

            @Override
            public MongoCursor<String> cursor() {
                return null;
            }

            @Override
            public String first() {
                return "";
            }

            @Override
            public <U> MongoIterable<U> map(Function<String, U> function) {
                return null;
            }

            @Override
            public <A extends Collection<? super String>> A into(A objects) {
                return null;
            }

            @Override
            public MongoIterable<String> batchSize(int i) {
                return null;
            }
        });
        when(mongoClient.getDatabase("testDB")).thenReturn(mongoDatabase);
        List<?> collections = mongoOperator.listCollections(true);
        assertEquals(2, collections.size());
        assertEquals("collection1", collections.get(0));
        assertEquals("collection2", collections.get(1));
    }

    @Test
    void testCount() {
        when(mongoClient.getDatabase("testDB")).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
        when(mongoCollection.countDocuments(any(Document.class), any(CountOptions.class))).thenReturn(10L);

        Map<String, Object> params = new HashMap<>();
        params.put("query", new Document("field", "value"));

        long count = mongoOperator.count("testCollection", params);
        assertEquals(10L, count);
    }

    @Test
    void testQuery() {
        when(mongoClient.getDatabase("testDB")).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
        when(mongoCollection.find(any(Document.class))).thenReturn(findIterable);
        when(findIterable.projection(any(Document.class))).thenReturn(findIterable);
        when(findIterable.limit(anyInt())).thenReturn(findIterable);
        when(findIterable.skip(anyInt())).thenReturn(findIterable);
        when(findIterable.maxTime(anyLong(), any())).thenReturn(findIterable);
        when(findIterable.cursor()).thenReturn(mongoCursor);
        when(mongoCursor.hasNext()).thenReturn(true, false);
        when(mongoCursor.next()).thenReturn(new Document("field", "value"));

        Map<String, Object> params = new HashMap<>();
        params.put("filter", new Document("field", "value"));

        List<Document> result = mongoOperator.query("testCollection", params);
        assertEquals(1, result.size());
        assertEquals("value", result.get(0).get("field"));
    }

    @Test
    void testAggregate() {
        when(mongoClient.getDatabase("testDB")).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("testCollection")).thenReturn(mongoCollection);
        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        when(aggregateIterable.cursor()).thenReturn(mongoCursor);
        when(mongoCursor.hasNext()).thenReturn(true, false);
        when(mongoCursor.next()).thenReturn(new Document("field", "value"));

        Map<String, Object> params = new HashMap<>();
        params.put("pipeline", Arrays.asList(new Document("$match", new Document("field", "value"))));

        List<Document> result = mongoOperator.aggregate("testCollection", params);
        assertEquals(1, result.size());
        assertEquals("value", result.get(0).get("field"));
    }
}
