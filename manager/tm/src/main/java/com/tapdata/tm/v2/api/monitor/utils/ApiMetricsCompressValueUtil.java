package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.entity.WorkerInfo;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import lombok.Getter;
import org.bson.Document;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/29 18:27 Create
 * @description
 */
public final class ApiMetricsCompressValueUtil {
    public static final String KEY = "k";
    public static final String VALUE = "v";

    public static Double getNum(Document entity, String key) {
        Object val = entity.get(key);
        if (val instanceof Double iDouble) {
            return value(iDouble).doubleValue();
        } else if (val instanceof Long iLong) {
            return iLong.doubleValue();
        }
        return 0D;
    }

    public static Number value(Number val) {
        if (null == val) {
            return 0L;
        }
        return ApiMetricsDelayUtil.parseKey(val).doubleValue();
    }

    private ApiMetricsCompressValueUtil() {

    }

    public static List<Map<String, Number>> asInit(Number k) {
        Map<String, Number> iMap = new HashMap<>();
        iMap.put(KEY, ApiMetricsDelayUtil.parseKey(k));
        iMap.put(VALUE, 1);
        return List.of(iMap);
    }

    public static boolean checkByCode(String code, String tag) {
        if (null == code) {
            return false;
        }
        int codNumber;
        try {
            codNumber = Integer.parseInt(code);
        } catch (NumberFormatException e) {
            return false;
        }
        return codNumber >= 200 && codNumber < 300;
    }


    public static <T> void calculate(List<T> items, Function<T, List<Map<String, Number>>> itemGetter, Setter setter) {
        if (CollectionUtils.isEmpty(items)) {
            return;
        }
        final List<Map<String, Number>> merge = mergeItems(items, itemGetter);
        ApiMetricsDelayUtil.Sum sumOf = ApiMetricsDelayUtil.sum(merge);
        final double sum = sumOf.getTotal();
        final long reqCount = sumOf.getCount();
        final Double p95 = ApiMetricsDelayUtil.p95(merge, reqCount);
        final Double p99 = ApiMetricsDelayUtil.p99(merge, reqCount);
        ApiMetricsDelayUtil.readMaxAndMin(merge, setter.getMaxSetter(), setter.getMinSetter());
        Optional.ofNullable(setter.getValueSetter()).ifPresent(s -> s.accept(sum));
        Optional.ofNullable(setter.getP95Setter()).ifPresent(s -> Optional.ofNullable(p95).ifPresent(s::accept));
        Optional.ofNullable(setter.getP99Setter()).ifPresent(s -> Optional.ofNullable(p99).ifPresent(s::accept));
        Optional.ofNullable(setter.getAvgSetter()).ifPresent(s -> s.accept(reqCount > 0L ? (1.0D * sum / reqCount) : 0D));
    }

    public static <T> List<Map<String, Number>> mergeItems(List<T> items, Function<T, List<Map<String, Number>>> itemGetter) {
        if (CollectionUtils.isEmpty(items)) {
            return new ArrayList<>();
        }
        final List<Map<String, Number>>[] delays = new List[items.size()];
        for (int i = 0; i < items.size(); i++) {
            final T info = items.get(i);
            if (null == info) {
                continue;
            }
            delays[i] = itemGetter.apply(info);
        }
        return ApiMetricsDelayUtil.merge(delays);
    }

    public static Double rate(Long value, Long right) {
        if (null == right || null == value) {
            return null;
        }
        if (right <= 0L) {
            return 0D;
        }
        return 100.0D * value / right;
    }

    public static long stepByGranularity(TimeGranularity granularity) {
        return switch (granularity) {
            case MINUTE -> TimeGranularity.MINUTE.getSeconds();
            case HOUR -> TimeGranularity.HOUR.getSeconds();
            default -> TimeGranularity.SECOND_FIVE.getSeconds();
        };
    }

    @Getter
    public static class Setter {
        DoubleConsumer valueSetter;
        DoubleConsumer maxSetter;
        DoubleConsumer minSetter;
        DoubleConsumer p95Setter;
        DoubleConsumer p99Setter;
        DoubleConsumer avgSetter;

        private Setter(DoubleConsumer valueSetter) {
            this.valueSetter = valueSetter;
        }

        public static Setter of(DoubleConsumer valueSetter) {
            return new Setter(valueSetter);
        }

        public Setter max(DoubleConsumer maxSetter) {
            this.maxSetter = maxSetter;
            return this;
        }

        public Setter min(DoubleConsumer minSetter) {
            this.minSetter = minSetter;
            return this;
        }

        public Setter p95(DoubleConsumer p95Setter) {
            this.p95Setter = p95Setter;
            return this;
        }

        public Setter p99(DoubleConsumer p99Setter) {
            this.p99Setter = p99Setter;
            return this;
        }

        public Setter avg(DoubleConsumer avgSetter) {
            this.avgSetter = avgSetter;
            return this;
        }
    }

    public static long sum(Long left, Long right) {
        if (null == left && null == right) {
            return 0L;
        }
        if (null == left) {
            return right;
        }
        if (null == right) {
            return left;
        }
        return left + right;
    }


    public static List<ApiMetricsRaw> minuteCompressToHour(List<ApiMetricsRaw> minuteData, TimeGranularity timeGranularity) {
        Map<Object, List<ApiMetricsRaw>> collect = minuteData.stream()
                .collect(Collectors.groupingBy(
                        e -> String.format("%s:%s:%s:%d", e.getApiId(), e.getReqPath(), e.getProcessId(), e.getMetricType())
                ));
        List<ApiMetricsRaw> hours = new ArrayList<>(collect.size());
        collect.forEach((key, items) -> {
            ApiMetricsRaw firstOne = items.get(0);
            ApiMetricsRaw hour = ApiMetricsRaw.instance(
                    firstOne.getProcessId(),
                    firstOne.getReqPath(),
                    firstOne.getApiId(),
                    timeGranularity.fixTime(firstOne.getTimeStart()),
                    timeGranularity,
                    MetricTypes.of(firstOne.getMetricType())
            );
            mergeAll(items, ApiMetricsRaw::getBytes, hour::setBytes);
            mergeAll(items, ApiMetricsRaw::getDelay, hour::setDelay);
            mergeAll(items, ApiMetricsRaw::getDbCost, hour::setDbCost);
            long errorCount = items.stream()
                    .filter(Objects::nonNull)
                    .filter(e -> Objects.nonNull(e.getErrorCount()))
                    .mapToLong(ApiMetricsRaw::getErrorCount)
                    .sum();
            ApiMetricsDelayUtil.Sum sumOf = ApiMetricsDelayUtil.sum(hour.getDelay());
            final double sum = sumOf.getTotal();
            final long reqCount = sumOf.getCount();
            final Double p95 = ApiMetricsDelayUtil.p95(hour.getDelay(), reqCount);
            final Double p99 = ApiMetricsDelayUtil.p99(hour.getDelay(), reqCount);
            ApiMetricsDelayUtil.readMaxAndMin(hour.getDelay(), hour::setMaxDelay, hour::setMinDelay);
            hour.setP95(p95);
            hour.setP99(p99);
            hour.setTtlKey(new Date(hour.getTimeStart() * 1000L));
            hour.setErrorCount(errorCount);
            hour.setReqCount(reqCount);
            List<WorkerInfo> mergedWorkers = items.stream()
                    .map(ApiMetricsRaw::getWorkerInfoMap)
                    .filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(
                            WorkerInfo::getWorkerOid,
                            Collectors.collectingAndThen(
                                    Collectors.toList(),
                                    ApiMetricsCompressValueUtil::mergeWorkerInfos
                            )
                    ))
                    .values()
                    .stream()
                    .toList();
            hour.setWorkerInfoMap(mergedWorkers);
            hour.setRps(reqCount > 0L ? (1.0D * sum / reqCount) : 0D);
            hours.add(hour);
        });
        return hours;
    }

    static WorkerInfo mergeWorkerInfos(List<WorkerInfo> infos) {
        WorkerInfo merged = new WorkerInfo();
        merged.setWorkerOid(infos.get(0).getWorkerOid());
        merged.setErrorCount(infos.stream()
                .map(WorkerInfo::getErrorCount)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum());
        merged.setReqCount(infos.stream()
                .map(WorkerInfo::getReqCount)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum());
        return merged;
    }

    static void mergeAll(List<ApiMetricsRaw> minuteData, Function<ApiMetricsRaw, List<Map<String, Number>>> getter, Consumer<List<Map<String, Number>>> setter) {
        List<List<Map<String, Number>>> allData = new ArrayList<>();
        minuteData.forEach(e -> allData.add(getter.apply(e)));
        setter.accept(ApiMetricsDelayUtil.merge(allData));
    }
}
