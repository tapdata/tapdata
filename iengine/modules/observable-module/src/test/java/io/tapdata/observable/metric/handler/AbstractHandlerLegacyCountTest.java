package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.CounterSampler;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

class AbstractHandlerLegacyCountTest {
    private static final String SNAPSHOT_ROW_TOTAL = MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL;
    private static final BigInteger UNSIGNED_LONG_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    @Test
    void restoresLegacyUnknownRowCountAsZero() {
        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", new SampleCollector());
        doCallRealMethod().when(handler).getCounterSampler(Map.of(SNAPSHOT_ROW_TOTAL, UNSIGNED_LONG_MAX), SNAPSHOT_ROW_TOTAL);

        CounterSampler counter = handler.getCounterSampler(Map.of(SNAPSHOT_ROW_TOTAL, UNSIGNED_LONG_MAX), SNAPSHOT_ROW_TOTAL);
        assertEquals(BigInteger.ZERO, counter.value());
        counter.inc(12);
        assertEquals(BigInteger.valueOf(12), counter.value());
    }

    @Test
    void clearsCachedLegacyUnknownRowCount() {
        SampleCollector collector = new SampleCollector();
        collector.getCounterSampler(SNAPSHOT_ROW_TOTAL, -1);
        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", collector);
        doCallRealMethod().when(handler).getCounterSampler(Map.of(SNAPSHOT_ROW_TOTAL, -1L), SNAPSHOT_ROW_TOTAL);

        assertEquals(BigInteger.ZERO, handler.getCounterSampler(Map.of(SNAPSHOT_ROW_TOTAL, -1L), SNAPSHOT_ROW_TOTAL).value());
    }

    @Test
    void restoresDecimal128MappedHistoryAsZero() {
        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", new SampleCollector());
        Map<String, Number> values = Map.of(SNAPSHOT_ROW_TOTAL, new BigDecimal(UNSIGNED_LONG_MAX));
        doCallRealMethod().when(handler).getCounterSampler(values, SNAPSHOT_ROW_TOTAL);

        assertEquals(BigInteger.ZERO, handler.getCounterSampler(values, SNAPSHOT_ROW_TOTAL).value());
    }

    @Test
    void restoresJsonIntegerMinusOneAsZero() {
        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", new SampleCollector());
        Map<String, Number> values = Map.of(SNAPSHOT_ROW_TOTAL, -1);
        doCallRealMethod().when(handler).getCounterSampler(values, SNAPSHOT_ROW_TOTAL);

        assertEquals(BigInteger.ZERO, handler.getCounterSampler(values, SNAPSHOT_ROW_TOTAL).value());
    }

    @Test
    void restoresMinusOneWithTheJsonNumberTypeUsedByMetricRetrieval() throws Exception {
        Map<String, Number> values = new ObjectMapper().readValue(
                "{\"snapshotRowTotal\":-1}", new TypeReference<Map<String, Number>>() {});
        assertEquals(Integer.class, values.get(SNAPSHOT_ROW_TOTAL).getClass());

        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", new SampleCollector());
        doCallRealMethod().when(handler).getCounterSampler(values, SNAPSHOT_ROW_TOTAL);

        assertEquals(BigInteger.ZERO, handler.getCounterSampler(values, SNAPSHOT_ROW_TOTAL).value());
    }

    @Test
    void clearsCachedLegacyValueWhenHistoryIsUnavailable() {
        SampleCollector collector = new SampleCollector();
        collector.getCounterSampler(SNAPSHOT_ROW_TOTAL, -1);
        AbstractHandler handler = mock(AbstractHandler.class);
        ReflectionTestUtils.setField(handler, "collector", collector);
        doCallRealMethod().when(handler).getCounterSampler(Map.of(), SNAPSHOT_ROW_TOTAL);

        assertEquals(BigInteger.ZERO, handler.getCounterSampler(Map.of(), SNAPSHOT_ROW_TOTAL).value());
    }
}
