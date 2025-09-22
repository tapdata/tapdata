package com.tapdata.tm.apiServer.service.compress;

import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public interface Compress {

    Long compressTime(WorkerCallEntity e);

    long fixTme(long time);

    default void checkTimeRange(long from, long end) {
        int days = ((Number) ((end - from) / 1000 / 60 / 60 / 24)).intValue();
        if (days > 365) {
            throw new BizException("api.call.chart.time.range.too.large", 365);
        }
    }

    default List<MetricDataBase> compress(List<WorkerCallEntity> items, Calculate compressor, MockItem<? extends MetricDataBase> mockItem) {
        final List<MetricDataBase> result = new ArrayList<>();
        items.stream()
                .collect(
                        Collectors.groupingBy(
                                this::compressTime,
                                Collectors.toList())
                ).forEach((key, values) -> result.add(compressor.mergeTo(key, values)));
        result.sort(Comparator.comparing(MetricDataBase::getTime));
        this.fixTime(result, mockItem);
        result.sort(Comparator.comparing(MetricDataBase::getTime).reversed());
        return result;
    }

    default void fixTime(List<MetricDataBase> result, MockItem<? extends MetricDataBase> mockItem) {
        List<MetricDataBase> addItem = new ArrayList<>();
        if (result.isEmpty()) {
            return;
        }
        long start = result.get(0).getTime();
        for (MetricDataBase metricDataBase : result) {
            start = plus(start);
            while (metricDataBase.getTime() > start) {
                addItem.add(mockItem.mock(start));
                start = plus(start);
            }
            start = metricDataBase.getTime();
        }
        if (!addItem.isEmpty()) {
            result.addAll(addItem);
        }
    }

    default void fixTime(ApiCallMetricVo vo, MockItem<? extends MetricDataBase> mockItem, long from, long end) {
        ApiCallMetricVo.ProcessMetric processMetric = vo.getProcessMetric();
        ApiCallMetricVo.MetricBase metricOfProcess = processMetric.getProcessMetric();
        fixBoundTime(metricOfProcess, mockItem, from, end, this::plus);
        List<ApiCallMetricVo.WorkerMetrics> workerMetrics = vo.getWorkerMetrics();
        for (ApiCallMetricVo.WorkerMetrics workerMetric : workerMetrics) {
            ApiCallMetricVo.MetricBase metricOfWorker = workerMetric.getWorkerMetric();
            fixBoundTime(metricOfWorker, mockItem, from, end, this::plus);
        }
    }

    /**
     * 补充左右边界缺失的点
     * */
    static void fixBoundTime(ApiCallMetricVo.MetricBase metric,
                             MockItem<? extends MetricDataBase> mockItem,
                             long from, long end,
                             ToLongFunction<Long> plus) {
        try {
            if (metric.getTime().isEmpty()) {
                while (from < end) {
                    MetricDataBase mock = mockItem.mock(from);
                    metric.add(null, from, mock.values());
                    from = plus.applyAsLong(from);
                }
                return;
            }
            metric.sort(1);
            Long first = metric.getTime().get(0);
            Long last = metric.getTime().get(metric.getTime().size() - 1);
            if (Objects.nonNull(first)) {
                while (from < first) {
                    MetricDataBase mock = mockItem.mock(from);
                    metric.add(0, from, mock.values());
                    from = plus.applyAsLong(from);
                }
            }
            if (Objects.nonNull(last)) {
                last = plus.applyAsLong(last);
                while (last < end) {
                    MetricDataBase mock = mockItem.mock(from);
                    metric.add(null, last, mock.values());
                    last = plus.applyAsLong(last);
                }
            }
        } finally {
            metric.sort(-1);
        }
    }

    long defaultFrom(long end);

    long plus(long time);

    interface Calculate {
        MetricDataBase mergeTo(Long timeStart, List<WorkerCallEntity> items);
    }

    interface MockItem<T extends MetricDataBase> {
        T mock(long time);
    }

    static Compress call(int type) {
        return Factory.get(Type.by(type));
    }

    class Factory {
        private Factory() {

        }

        private static final Map<Type, Compress> instances = new EnumMap<>(Compress.Type.class);

        public static Compress get(Compress.Type type) {
            return instances.get(type);
        }

        public static void register(Compress.Type type, Compress compress) {
            instances.put(type, compress);
        }
    }

    @Getter
    enum Type {
        MINUTE(0), HOUR(1), DAY(2), WEEK(3), MONTH(4);
        final int code;

        Type(int code) {
            this.code = code;
        }

        public static Type by(int code) {
            for (Type value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            return MINUTE;
        }
    }
}
