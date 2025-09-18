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

class MetricErrorRateTest {

    @Test
    void testFields() {
        Query query = new Query();
        MetricErrorRate metric = new MetricErrorRate();
        metric.fields(query);
        Set<String> fields = query.fields().getFieldsObject().keySet();
        Assertions.assertEquals(6, fields.size());
        Assertions.assertTrue(fields.contains("errorRate"));
        Assertions.assertTrue(fields.contains("errorCount"));
        Assertions.assertTrue(fields.contains("reqCount"));
        Assertions.assertTrue(fields.contains("workOid"));
        Assertions.assertTrue(fields.contains("timeStart"));
        Assertions.assertTrue(fields.contains("processId"));
    }

    @Test
    void testMergeTo() {
        MetricErrorRate metric = new MetricErrorRate();
        MetricDataBase metricDataBase = metric.mergeTo(0L, new ArrayList<>());
        Assertions.assertNotNull(metricDataBase);
    }
    @Test
    void testMergeTo1() {
        List<WorkerCallEntity> vos = new ArrayList<>();
        WorkerCallEntity entity = new WorkerCallEntity();
        entity.setReqCount(1L);
        entity.setErrorCount(1L);
        vos.add(entity);
        MetricErrorRate metric = new MetricErrorRate();
        MetricDataBase metricDataBase = metric.mergeTo(0L, vos);
        Assertions.assertNotNull(metricDataBase);
    }
    @Test
    void testToResult() {
        MetricErrorRate metric = new MetricErrorRate();
        List<MetricDataBase> data = new ArrayList<>();
        data.add(null);
        data.add(metric.mock(0L));
        ApiCallMetricVo.MetricErrorRate result = metric.toResult(data);
        Assertions.assertNotNull(result);
    }

    @Test
    void testAfterPropertiesSet() {
        MetricErrorRate metric = new MetricErrorRate();
        metric.afterPropertiesSet();
        Assertions.assertNotNull(Metric.Factory.get(Metric.Type.ERROR_RATE));
    }
}