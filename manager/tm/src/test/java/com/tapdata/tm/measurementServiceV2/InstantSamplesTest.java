package com.tapdata.tm.measurementServiceV2;

import com.mongodb.client.MongoClient;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

 class InstantSamplesTest {

    public static MeasurementServiceV2 measurementServiceV2;

    public static MockMongoTemplate mongoTemplate;
    @BeforeAll
     static void init() throws ExecutionException, InterruptedException {
        CompletableFuture<MongoTemplate> mongoOperations = new CompletableFuture<>();
        MongoClient mongoClient = Mockito.mock(MongoClient.class);
        mongoTemplate = new MockMongoTemplate(mongoClient, "test");
        mongoOperations.complete(mongoTemplate);
        MetadataInstancesService metadataInstancesService = Mockito.mock(MetadataInstancesService.class);
        TaskService taskService = Mockito.mock(TaskService.class);
        measurementServiceV2 = new MeasurementServiceV2(mongoOperations, metadataInstancesService, taskService);
    }


    /**
     * 单侧testGetInstantSamp 方法，用一个MockMongoTemplate获取查询的的值。来判断type不一样查询中 LimitOperation输入
     * 本方法是测试node。期望是不存在限制
     */
    @ParameterizedTest
    @CsvSource({"node,false", "engine,true", "task,true"})
    void testGetInstantSamples(String type,boolean expectedData) {
        // exec function
        System.out.println(type+"---"+expectedData);
        boolean actualData = execFunction(type);

        // compare result
        Assertions.assertEquals(expectedData, actualData);

    }

     boolean execFunction(String type) {
        // input query param
        MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("type", type);
        querySample.setTags(tags);
        querySample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT);
        ReflectionTestUtils.invokeMethod(measurementServiceV2, "getInstantSamples", querySample, "test", 1700133178786L, 1700133478786L);
        Aggregation aggregation = mongoTemplate.getAggregation();
        AtomicBoolean actualData = new AtomicBoolean(false);

        // exec function
        aggregation.getPipeline().getOperations().stream().forEach(operation -> {
            if (operation instanceof LimitOperation) {
                actualData.set(true);
            }
        });
        return actualData.get();

    }


}
