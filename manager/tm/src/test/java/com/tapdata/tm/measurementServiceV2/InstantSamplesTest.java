package com.tapdata.tm.measurementServiceV2;

import com.mongodb.client.MongoClient;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

public class InstantSamplesTest {

    public static MeasurementServiceV2 measurementServiceV2;

    public static MockMongoTemplate mongoTemplate;
    @BeforeAll
    public static void init() throws ExecutionException, InterruptedException {
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
    @Test
    public void testGetInstantSamplesForNode() {
        // exec function
        boolean actualData = execFunction("node");

        // expected data
        boolean expectedData = false;

        // compare result
        Assertions.assertEquals(expectedData, actualData);

    }


    /**
     * 单侧testGetInstantSamp 方法，用一个MockMongoTemplate获取查询的的值。来判断type不一样查询中 LimitOperation输入
     * 本方法是测试engine。期望是存在限制
     */
    @Test
    public void testGetInstantSamplesForEngine() {

        // exec function
        boolean actualData = execFunction("engine");

        // expected data
        boolean expectedData = true;

        // compare result
        Assertions.assertEquals(expectedData, actualData);

    }

    /**
     * 单侧testGetInstantSamp 方法，用一个MockMongoTemplate获取查询的的值。来判断type不一样查询中 LimitOperation输入
     * 本方法是测试task。期望是存在限制
     */
    @Test
    public void testGetInstantSamplesForTask(){

        // exec function
        boolean actualData = execFunction("task");

        // expected data
        boolean expectedData = true;

        // compare result
        Assertions.assertEquals(expectedData, actualData);

    }


    public boolean execFunction(String type) {
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
