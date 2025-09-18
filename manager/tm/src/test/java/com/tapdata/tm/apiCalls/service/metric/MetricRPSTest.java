package com.tapdata.tm.apiCalls.service.metric;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.apiServer.service.metric.Metric;
import com.tapdata.tm.apiServer.service.metric.MetricRPS;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class MetricRPSTest {
    @Test
    void testFields() {
        Query query = new Query();
        MetricRPS metric = new MetricRPS();
        metric.fields(query);
        Set<String> fields = query.fields().getFieldsObject().keySet();
        Assertions.assertEquals(5, fields.size());
        Assertions.assertTrue(fields.contains("rps"));
        Assertions.assertTrue(fields.contains("reqCount"));
        Assertions.assertTrue(fields.contains("workOid"));
        Assertions.assertTrue(fields.contains("timeStart"));
        Assertions.assertTrue(fields.contains("processId"));
    }

    @Test
    void testMergeTo() {
        MetricRPS metric = new MetricRPS();
        MetricDataBase metricDataBase = metric.mergeTo(0L, new ArrayList<>());
        Assertions.assertNotNull(metricDataBase);
    }

    @Test
    void testMergeTo1() {
        List<WorkerCallEntity> vos = new ArrayList<>();
        WorkerCallEntity entity = new WorkerCallEntity();
        entity.setTimeStart(0L);
        vos.add(entity);
        WorkerCallEntity e1 = new WorkerCallEntity();
        e1.setTimeStart(50000L);
        vos.add(e1);
        WorkerCallEntity e2 = new WorkerCallEntity();
        e2.setTimeStart(13980084L);
        vos.add(e2);
        WorkerCallEntity e3 = new WorkerCallEntity();
        vos.add(e3);
        MetricRPS metric = new MetricRPS();
        MetricDataBase metricDataBase = metric.mergeTo(0L, vos);
        Assertions.assertNotNull(metricDataBase);
    }

    @Test
    void testToResult() {
        MetricRPS metric = new MetricRPS();
        List<MetricDataBase> data = new ArrayList<>();
        data.add(null);
        data.add(metric.mock(0L));
        ApiCallMetricVo.MetricRPS result = metric.toResult(data);
        Assertions.assertNotNull(result);
    }

    @Test
    void testAfterPropertiesSet() {
        MetricRPS metric = new MetricRPS();
        metric.afterPropertiesSet();
        Assertions.assertNotNull(Metric.Factory.get(Metric.Type.RPS));
    }
}