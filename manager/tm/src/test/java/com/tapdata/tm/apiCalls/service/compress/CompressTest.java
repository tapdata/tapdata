package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.service.compress.Compress;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.apiServer.vo.metric.OfErrorRate;
import com.tapdata.tm.base.exception.BizException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class CompressTest {
    class MockCompress implements Compress {
        @Override
        public Long compressTime(WorkerCallEntity e) {
            return e.getTimeStart();
        }

        @Override
        public long fixTme(long time) {
            return 0;
        }

        @Override
        public long defaultFrom(long end) {
            return 0;
        }

        @Override
        public long plus(long time) {
            return time + 1000L;
        }
    }

    @Test
    void testCompress() {
        Compress compress = new MockCompress();
        List<WorkerCallEntity> items = new ArrayList<>();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1L);
        items.add(e);
        List<MetricDataBase> result = compress.compress(items, (timeStart, values) -> {
            MetricDataBase metric = new MetricDataBase();
            metric.setTime(timeStart);
            return metric;
        }, time -> new MetricDataBase());
        Assertions.assertNotNull(result);
    }

    @Test
    void testType() {
        Assertions.assertEquals(Compress.Type.MINUTE, Compress.Type.by(0));
        Assertions.assertEquals(Compress.Type.HOUR, Compress.Type.by(1));
        Assertions.assertEquals(Compress.Type.DAY, Compress.Type.by(2));
        Assertions.assertEquals(Compress.Type.WEEK, Compress.Type.by(3));
        Assertions.assertEquals(Compress.Type.MONTH, Compress.Type.by(4));
        Assertions.assertEquals(Compress.Type.MINUTE, Compress.Type.by(5));
        Assertions.assertEquals(Compress.Type.MINUTE, Compress.Type.by(-1));
    }

    @Test
    void testFactory() {
        Compress.Factory.register(Compress.Type.MINUTE, new MockCompress());
        Assertions.assertNotNull(Compress.Factory.get(Compress.Type.MINUTE));
    }

    @Test
    void testFixTime() {
        Compress compress = new MockCompress();
        List<MetricDataBase> result = new ArrayList<>();
        MetricDataBase metric = new MetricDataBase();
        metric.setTime(1000L);
        result.add(metric);

        MetricDataBase metric1 = new MetricDataBase();
        metric1.setTime(5000L);
        result.add(metric1);
        compress.fixTime(result, time -> new MetricDataBase());
        Assertions.assertNotNull(result);
    }

    @Test
    void testFixTime2() {
        ApiCallMetricVo vo = new ApiCallMetricVo();
        ApiCallMetricVo.ProcessMetric processMetric = new ApiCallMetricVo.ProcessMetric();
        vo.setProcessMetric(processMetric);
        ApiCallMetricVo.MetricBase metricOfProcess = new ApiCallMetricVo.MetricErrorRate();
        processMetric.setProcessMetric(metricOfProcess);
        List<ApiCallMetricVo.WorkerMetrics> workerMetrics = new ArrayList<>();
        vo.setWorkerMetrics(workerMetrics);
        ApiCallMetricVo.WorkerMetrics workerMetric = new ApiCallMetricVo.WorkerMetrics();
        workerMetrics.add(workerMetric);
        ApiCallMetricVo.MetricBase metricOfWorker = new ApiCallMetricVo.MetricErrorRate();
        workerMetric.setWorkerMetric(metricOfWorker);
        Compress compress = new MockCompress();
        compress.fixTime(vo, time -> new MetricDataBase(), 0L, 5000L);
        Assertions.assertNotNull(vo.getWorkerMetrics());
        Assertions.assertEquals(5, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().size());
        Assertions.assertEquals(4000L, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().get(0));
        Assertions.assertEquals(3000L, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().get(1));
        Assertions.assertEquals(2000L, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().get(2));
        Assertions.assertEquals(1000L, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().get(3));
        Assertions.assertEquals(0L, vo.getWorkerMetrics().get(0).getWorkerMetric().getTime().get(4));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getWorkerMetrics().get(0).getWorkerMetric()).getErrorRate().get(0));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getWorkerMetrics().get(0).getWorkerMetric()).getErrorRate().get(1));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getWorkerMetrics().get(0).getWorkerMetric()).getErrorRate().get(2));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getWorkerMetrics().get(0).getWorkerMetric()).getErrorRate().get(3));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getWorkerMetrics().get(0).getWorkerMetric()).getErrorRate().get(4));

        Assertions.assertEquals(5, vo.getProcessMetric().getProcessMetric().getTime().size());
        Assertions.assertEquals(4000L, vo.getProcessMetric().getProcessMetric().getTime().get(0));
        Assertions.assertEquals(3000L, vo.getProcessMetric().getProcessMetric().getTime().get(1));
        Assertions.assertEquals(2000L, vo.getProcessMetric().getProcessMetric().getTime().get(2));
        Assertions.assertEquals(1000L, vo.getProcessMetric().getProcessMetric().getTime().get(3));
        Assertions.assertEquals(0L, vo.getProcessMetric().getProcessMetric().getTime().get(4));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getProcessMetric().getProcessMetric()).getErrorRate().get(0));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getProcessMetric().getProcessMetric()).getErrorRate().get(1));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getProcessMetric().getProcessMetric()).getErrorRate().get(2));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getProcessMetric().getProcessMetric()).getErrorRate().get(3));
        Assertions.assertNull(((ApiCallMetricVo.MetricErrorRate)vo.getProcessMetric().getProcessMetric()).getErrorRate().get(4));
    }

    @Nested
    class CheckTimeRangeTest {
        @Test
        void test1() {
            Assertions.assertDoesNotThrow(() -> new MockCompress().checkTimeRange(0L, 0L));
        }

        @Test
        void test2() {
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    new MockCompress().checkTimeRange(0L, 1000L * 60 * 60 * 24 * 366);
                } catch (BizException e) {
                    Assertions.assertEquals("api.call.chart.time.range.too.large", e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Nested
    class FixBoundTimeTest {

        @Test
        void testNormal() {
            ApiCallMetricVo.MetricErrorRate metric = new ApiCallMetricVo.MetricErrorRate();
            metric.add(null, 2000L, 0.2d);
            metric.add(null, 4000L, 0.1d);
            Compress.fixBoundTime(metric, time -> new OfErrorRate(), 0L, 6000L, time -> time + 1000L);
            Assertions.assertEquals(5, metric.getTime().size());
            Assertions.assertNull(metric.getErrorRate().get(4));
            Assertions.assertNull(metric.getErrorRate().get(3));
            Assertions.assertEquals(0.2d, metric.getErrorRate().get(2));
            Assertions.assertEquals(0.1d, metric.getErrorRate().get(1));
            Assertions.assertNull(metric.getErrorRate().get(0));
        }

        @Test
        void testEmpty() {
            ApiCallMetricVo.MetricErrorRate metric = new ApiCallMetricVo.MetricErrorRate();
            Compress.fixBoundTime(metric, time -> new OfErrorRate(), 0L, 5000L, time -> time + 1000L);
            Assertions.assertEquals(5, metric.getTime().size());
            Assertions.assertNull(metric.getErrorRate().get(0));
            Assertions.assertNull(metric.getErrorRate().get(1));
            Assertions.assertNull(metric.getErrorRate().get(2));
            Assertions.assertNull(metric.getErrorRate().get(3));
            Assertions.assertNull(metric.getErrorRate().get(4));
        }
    }
}