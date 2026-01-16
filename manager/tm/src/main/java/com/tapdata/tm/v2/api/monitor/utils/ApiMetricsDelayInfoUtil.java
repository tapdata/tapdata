package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import lombok.Getter;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/29 18:27 Create
 * @description
 */
public final class ApiMetricsDelayInfoUtil {

    private ApiMetricsDelayInfoUtil() {

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
        if (codNumber == 404) {
            return !ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode().equals(tag);
        }
        return codNumber >= 200 && codNumber < 300;
    }


    public static <T> void calculate(List<T> items, Function<T, List<?>> itemGetter, Setter setter) {
        if (CollectionUtils.isEmpty(items)) {
            return;
        }
        final List<Map<Long, Integer>>[] delays = new List[items.size()];
        for (int i = 0; i < items.size(); i++) {
            final T info = items.get(i);
            if (null == info) {
                continue;
            }
            delays[i] = ApiMetricsDelayUtil.fixDelayAsMap(itemGetter.apply(info));
        }
        final List<Map<Long, Integer>> merge = ApiMetricsDelayUtil.merge(delays);
        final long sum = ApiMetricsDelayUtil.sum(merge);
        final long total = ApiMetricsDelayUtil.sum(merge, (k, v) -> v.longValue());
        final Long p95 = ApiMetricsDelayUtil.p95(merge, total);
        final Long p99 = ApiMetricsDelayUtil.p99(merge, total);
        ApiMetricsDelayUtil.readMaxAndMin(merge, setter.getMaxSetter(), setter.getMinSetter());
        Optional.ofNullable(setter.getValueSetter()).ifPresent(s -> s.accept(sum));
        Optional.ofNullable(setter.getP95Setter()).ifPresent(s -> Optional.ofNullable(p95).ifPresent(s::accept));
        Optional.ofNullable(setter.getP99Setter()).ifPresent(s -> Optional.ofNullable(p99).ifPresent(s::accept));
        Optional.ofNullable(setter.getAvgSetter()).ifPresent(s -> s.accept(total > 0L ? (1.0D * sum / total) : 0D));
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

    @Getter
    public static class Setter {
        LongConsumer valueSetter;
        LongConsumer maxSetter;
        LongConsumer minSetter;
        LongConsumer p95Setter;
        LongConsumer p99Setter;
        DoubleConsumer avgSetter;
        private Setter(LongConsumer valueSetter) {
            this.valueSetter = valueSetter;
        }
        public static Setter of(LongConsumer valueSetter) {
            return new Setter(valueSetter);
        }

        public Setter max(LongConsumer maxSetter) {
            this.maxSetter = maxSetter;
            return this;
        }

        public Setter min(LongConsumer minSetter) {
            this.minSetter = minSetter;
            return this;
        }

        public Setter p95(LongConsumer p95Setter) {
            this.p95Setter = p95Setter;
            return this;
        }

        public Setter p99(LongConsumer p99Setter) {
            this.p99Setter = p99Setter;
            return this;
        }

        public Setter avg(DoubleConsumer avgSetter) {
            this.avgSetter = avgSetter;
            return this;
        }
    }
}
