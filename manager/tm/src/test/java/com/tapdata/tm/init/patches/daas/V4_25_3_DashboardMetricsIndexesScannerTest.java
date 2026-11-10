package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.IPatch;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.PatchesRunner;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import io.tapdata.utils.AppType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V4_25_3_DashboardMetricsIndexesScannerTest {

    @Test
    void scannerSelectsPatchAfterTheCurrentIdaasVersion() {
        List<IPatch> patches = new ArrayList<>();
        PatchVersion sourceVersion = PatchVersion.valueOf("4.25-3");
        PatchVersion storedVersion = PatchVersion.valueOf("4.25-2");
        JavaPatchScanner scanner = new JavaPatchScanner(AppType.DAAS);

        scanner.scanPatches(patches,
                PatchesRunner.getVersionChecker(AppType.DAAS, sourceVersion, storedVersion));

        assertTrue(patches.stream().anyMatch(V4_25_3_DashboardMetricsIndexes.class::isInstance));
    }

    @Test
    void conflictingIndexNameDoesNotFailThePatch() {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        List<Document> indexes = List.of(new Document("name", V4_25_3_DashboardMetricsIndexes.MEASUREMENT_LATE_ROLLUP_INDEX)
                .append("key", new Document("other", 1)));

        Assertions.assertDoesNotThrow(() ->
                V4_25_3_DashboardMetricsIndexes.ensureLateTaskRollupIndex(collection, indexes));

        verify(collection, never()).createIndex(any(Bson.class), any(IndexOptions.class));
    }

    @Test
    void incompleteObsTemplateDoesNotBlockSchedulingAndFailureIsHandled() {
        java.util.concurrent.CompletableFuture<MongoTemplate> pending = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Void> result = Assertions.assertTimeoutPreemptively(
                java.time.Duration.ofSeconds(2),
                () -> V4_25_3_DashboardMetricsIndexes.initializeMeasurementIndexAsync(pending));
        Assertions.assertFalse(result.isDone());
        pending.completeExceptionally(new IllegalStateException("obs unavailable"));
        Assertions.assertDoesNotThrow(result::join);
    }

    @Test
    void indexCreationFailureIsLoggedInsteadOfBlockingStartup() {
        MongoTemplate template = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        @SuppressWarnings("unchecked")
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        when(template.getCollection(any())).thenReturn(collection);
        when(collection.listIndexes()).thenReturn(indexes);
        when(indexes.into(org.mockito.ArgumentMatchers.<List<Document>>any())).thenReturn(new ArrayList<>());
        when(collection.createIndex(any(Bson.class), any(IndexOptions.class)))
                .thenThrow(new IllegalStateException("mongo down"));

        Assertions.assertDoesNotThrow(() ->
                V4_25_3_DashboardMetricsIndexes.ensureApiMetricsNodeTrendIndex(template));

        verify(collection).createIndex(any(Bson.class), any(IndexOptions.class));
    }
}
