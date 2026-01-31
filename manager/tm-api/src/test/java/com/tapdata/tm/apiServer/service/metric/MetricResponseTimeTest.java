package com.tapdata.tm.apiServer.service.metric;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.apiServer.service.metric.Metric;
import com.tapdata.tm.apiServer.service.metric.MetricResponseTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class MetricResponseTimeTest {
    @Test
    void testFields() {
        Query query = new Query();
        MetricResponseTime metric = new MetricResponseTime();
        metric.fields(query);
        Set<String> fields = query.fields().getFieldsObject().keySet();
        Assertions.assertEquals(7, fields.size());
        Assertions.assertTrue(fields.contains("delays"));
        Assertions.assertTrue(fields.contains("p50"));
        Assertions.assertTrue(fields.contains("p95"));
        Assertions.assertTrue(fields.contains("p99"));
        Assertions.assertTrue(fields.contains("workOid"));
        Assertions.assertTrue(fields.contains("timeStart"));
        Assertions.assertTrue(fields.contains("processId"));
    }

    @Test
    void testMergeTo() {
        MetricResponseTime metric = new MetricResponseTime();
        MetricDataBase metricDataBase = metric.mergeTo(0L, new ArrayList<>());
        Assertions.assertNotNull(metricDataBase);
    }
    @Test
    void testToResult() {
        MetricResponseTime metric = new MetricResponseTime();
        List<MetricDataBase> data = new ArrayList<>();
        data.add(null);
        data.add(metric.mock(0L));
        ApiCallMetricVo.MetricResponseTime result = metric.toResult(data);
        Assertions.assertNotNull(result);
    }

    @Test
    void testAfterPropertiesSet() {
        MetricResponseTime metric = new MetricResponseTime();
        metric.afterPropertiesSet();
        Assertions.assertNotNull(Metric.Factory.get(Metric.Type.RESPONSE_TIME));
    }
}