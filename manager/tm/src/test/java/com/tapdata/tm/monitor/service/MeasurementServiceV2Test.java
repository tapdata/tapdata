package com.tapdata.tm.monitor.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.task.service.TaskService;
import io.github.openlg.graphlib.Graph;
import io.tapdata.common.sample.request.Sample;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MeasurementServiceV2Test {

    @Mock
    private MetadataInstancesService mockMetadataInstancesService;
    @Mock
    private TaskService mockTaskService;
    @Mock
    private MongoTemplate mockMongoOperations;

    private MeasurementServiceV2Impl measurementServiceV2UnderTest;
    @Mock
    private CompletableFuture<MongoTemplate> mongoTemplateCompletableFuture;



    @BeforeEach
    void setUp() throws Exception {
        when(mongoTemplateCompletableFuture.get()).thenReturn(mockMongoOperations);
        measurementServiceV2UnderTest = new MeasurementServiceV2Impl(
                mongoTemplateCompletableFuture, mockMetadataInstancesService,
                mockTaskService);
    }

    @Test
    void testGetInstantSamples() {
        final MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
        Map<String,String> mockTap = new HashMap<>();
        mockTap.put("type","node");
        mockTap.put("taskId","659fc0afbd39cd1272e51586");
        querySample.setTags(mockTap);
        querySample.setFields(Arrays.asList("value"));
        querySample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT);
        querySample.setStartAt(0L);
        querySample.setEndAt(0L);
        final MeasurementEntity measurementEntity = new MeasurementEntity();
        measurementEntity.setDate(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        measurementEntity.setTags(mockTap);
        final Sample sample = new Sample();
        sample.setVs(new HashMap<>());
        sample.setDate(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        measurementEntity.setSamples(Arrays.asList(sample));
        final AggregationResults<MeasurementEntity> measurementEntities = new AggregationResults<>(
                Arrays.asList(measurementEntity), new Document("$project", "value"));
        when(mockMongoOperations.aggregate(any(Aggregation.class), eq("AgentMeasurementV2"),
                eq(MeasurementEntity.class))).thenReturn(measurementEntities);
        final Map<String, Sample> result = measurementServiceV2UnderTest.getInstantSamples(querySample, "padding", 0L,
                0L);
        Assertions.assertNotNull(result.get("taskId:659fc0afbd39cd1272e51586;type:node;"));

    }

    @Test
    void testGetInstantSamples_TaskServiceReturnsNull() {
        final MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
        Map<String,String> mockTap = new HashMap<>();
        mockTap.put("type","node");
        mockTap.put("taskId","659fc0afbd39cd1272e51586");
        querySample.setTags(mockTap);
        querySample.setFields(Arrays.asList("value"));
        querySample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS);
        querySample.setStartAt(0L);
        querySample.setEndAt(0L);
        final Map<String, Sample> result = measurementServiceV2UnderTest.getInstantSamples(querySample, "padding", 0L,
                0L);
        Assertions.assertNull(result.get("taskId:659fc0afbd39cd1272e51586;type:node;"));
    }
}
