package com.tapdata.tm.config;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ObsMongoConfigTest {

    @Mock
    private MongoCollection<Document> collection;
    @Mock
    private ListIndexesIterable<Document> listIndexesIterable;

    private ObsMongoConfig config;

    @BeforeEach
    void setUp() {
        config = new ObsMongoConfig();
    }

    @Test
    void shouldCreateDashboardIndexesForNonEmptyCollection() {
        when(collection.estimatedDocumentCount()).thenReturn(100L);
        mockExistingIndexes();

        config.initializeAgentMeasurementIndexes(collection);

        ArgumentCaptor<Bson> keyCaptor = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<IndexOptions> optionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);
        verify(collection, times(2)).createIndex(keyCaptor.capture(), optionsCaptor.capture());
        verify(collection, never()).dropIndexes();

        Document throughputIndex = (Document) keyCaptor.getAllValues().get(0);
        Document latestIndex = (Document) keyCaptor.getAllValues().get(1);
        assertEquals(List.of("grnty", "tags.type", "tags.taskId", "date"),
                new ArrayList<>(throughputIndex.keySet()));
        assertEquals(List.of("tags.taskId", "grnty", "tags.type", "date"),
                new ArrayList<>(latestIndex.keySet()));
        assertEquals(new Document("grnty", 1)
                        .append("tags.type", 1)
                        .append("tags.taskId", 1)
                        .append("date", 1),
                throughputIndex);
        assertEquals(new Document("tags.taskId", 1)
                        .append("grnty", 1)
                        .append("tags.type", 1)
                        .append("date", -1),
                latestIndex);
        assertEquals(ObsMongoConfig.IDX_GRNTY_TYPE_TASK_ID_DATE, optionsCaptor.getAllValues().get(0).getName());
        assertEquals(ObsMongoConfig.IDX_TASK_ID_GRNTY_TYPE_DATE, optionsCaptor.getAllValues().get(1).getName());
    }

    @Test
    void shouldKeepBootstrapIndexesForEmptyCollection() {
        when(collection.estimatedDocumentCount()).thenReturn(0L);
        mockExistingIndexes();

        config.initializeAgentMeasurementIndexes(collection);

        verify(collection).dropIndexes();
        verify(collection, times(3)).createIndex(any(Bson.class));
        verify(collection, times(3)).createIndex(any(Bson.class), any(IndexOptions.class));
    }

    @Test
    void shouldSkipIndexesWhenSameKeysAlreadyExistUnderDifferentNames() {
        when(collection.estimatedDocumentCount()).thenReturn(100L);
        mockExistingIndexes(
                index("legacy-throughput-index", new Document("grnty", 1)
                        .append("tags.type", 1)
                        .append("tags.taskId", 1)
                        .append("date", 1)),
                index("legacy-latest-index", new Document("tags.taskId", 1)
                        .append("grnty", 1)
                        .append("tags.type", 1)
                        .append("date", -1)));

        config.initializeAgentMeasurementIndexes(collection);

        verify(collection, never()).createIndex(any(Bson.class), any(IndexOptions.class));
    }

    @Test
    void shouldSkipConflictingIndexAndContinueWithRemainingIndex() {
        when(collection.estimatedDocumentCount()).thenReturn(100L);
        mockExistingIndexes(index(ObsMongoConfig.IDX_GRNTY_TYPE_TASK_ID_DATE,
                new Document("grnty", 1)
                        .append("tags.taskId", 1)
                        .append("tags.type", 1)
                        .append("date", 1)));

        config.initializeAgentMeasurementIndexes(collection);

        Document latestIndex = new Document("tags.taskId", 1)
                .append("grnty", 1)
                .append("tags.type", 1)
                .append("date", -1);
        verify(collection).createIndex(eq(latestIndex),
                argThat(options -> ObsMongoConfig.IDX_TASK_ID_GRNTY_TYPE_DATE.equals(options.getName())));
        verify(collection, times(1)).createIndex(any(Bson.class), any(IndexOptions.class));
    }

    @Test
    void shouldPropagateIndexCreationFailure() {
        when(collection.estimatedDocumentCount()).thenReturn(100L);
        mockExistingIndexes();
        RuntimeException failure = new RuntimeException("index build failed");
        when(collection.createIndex(any(Bson.class), any(IndexOptions.class))).thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> config.initializeAgentMeasurementIndexes(collection));

        assertSame(failure, actual);
    }

    private void mockExistingIndexes(Document... indexes) {
        when(collection.listIndexes()).thenReturn(listIndexesIterable);
        when(listIndexesIterable.into(anyList())).thenAnswer(invocation -> {
            List<Document> target = invocation.getArgument(0);
            target.addAll(Arrays.asList(indexes));
            return target;
        });
    }

    private Document index(String name, Document keys) {
        return new Document("name", name).append("key", keys);
    }
}
