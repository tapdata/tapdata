package com.tapdata.tm.apiCalls.service.metric;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
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
    void testMergeTo1() {
        List<WorkerCallEntity> vos = new ArrayList<>();
        WorkerCallEntity entity = new WorkerCallEntity();
        entity.setDelays(List.of(2L, 4L));
        vos.add(entity);
        WorkerCallEntity e1 = new WorkerCallEntity();
        e1.setDelays(null);
        vos.add(e1);
        WorkerCallEntity e2 = new WorkerCallEntity();
        e2.setDelays(new ArrayList<>());
        vos.add(e2);
        MetricResponseTime metric = new MetricResponseTime();
        MetricDataBase metricDataBase = metric.mergeTo(0L, vos);
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