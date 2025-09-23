package com.tapdata.tm.apiServer.vo;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 12:06 Create
 * @description
 */
@Data
public class ApiCallMetricVo {

    private ProcessMetric processMetric = new ProcessMetric();

    private List<WorkerMetrics> workerMetrics = new ArrayList<>();

    /**
     * query end time
     * */
    private long endAs;

    private long startAs;

    @Data
    public static class ProcessMetric {
        private String processId;
        private String serverName;
        MetricBase processMetric;
    }

    @Data
    public static class WorkerMetrics {
        private String workOid;
        private String workerName;
        private MetricBase workerMetric;
    }


    @Data
    public static class MetricBase {
        List<Long> time;

        public MetricBase() {
            this.time = new ArrayList<>();
        }

        public void add(Integer index, Long time, Object... values) {
            if (index == null) {
                this.time.add(time);
                return;
            }
            this.time.add(index, time);
        }

        protected  <T extends Number> void apply(Object[] item, int index, Function<Number, T> to, Consumer<T> setter) {
            if (item.length > index && item[index] instanceof Number number) {
                setter.accept(to.apply(number));
                return;
            }
            setter.accept(null);
        }

       protected <E extends Number> void sortOne(int sortType, List<E> list, Comparator<? super E> c) {
            if (sortType > 0) {
                list.sort(c);
            } else if (sortType < 0) {
                list.sort(c.reversed());
            }
        }

        public void sort(int sortType) {
            if (sortType > 0) {
                this.time.sort(Long::compareTo);
            } else if (sortType < 0) {
                this.time.sort(Comparator.comparing(Long::longValue).reversed());
            }
        }

        protected Map<Long, Object[]> valuesMap() {
            Map<Long, Object[]> map = new HashMap<>();
            for (int i = 0; i < this.time.size(); i++) {
                map.put(this.time.get(i), values(i));
            }
            return map;
        }

        protected Object[] values(int index) {
            return new Object[]{};
        }

        protected Object get(List<?> list, int index) {
            return list.size() > index ? list.get(index) : null;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricRPS extends MetricBase {
        List<Double> rps;

        public MetricRPS() {
            super();
            this.rps = new ArrayList<>();
        }

        @Override
        public void add(Integer index, Long time, Object... values) {
            super.add(index, time);
            apply(values, 0, Number::doubleValue, null == index ? this.rps::add : v -> this.rps.add(index, v));
        }

        @Override
        public void sort(int sort) {
            Map<Long, Object[]> valuesMap = this.valuesMap();
            super.sort(sort);
            this.rps = new ArrayList<>();
            for (Long time : this.time) {
                Object[] values = Optional.ofNullable(valuesMap.get(time)).orElse(new Object[]{null});
                apply(values, 0, Number::doubleValue, this.rps::add);
            }
        }

        @Override
        protected Object[] values(int index) {
            return new Object[]{get(this.rps, index)};
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricResponseTime extends MetricBase {
        List<Long> p50;
        List<Long> p95;
        List<Long> p99;

        public MetricResponseTime() {
            super();
            this.p50 = new ArrayList<>();
            this.p95 = new ArrayList<>();
            this.p99 = new ArrayList<>();
        }

        @Override
        public void add(Integer index, Long time, Object... values) {
            super.add(index, time);
            apply(values, 0, Number::longValue, null == index ? this.p50::add : v -> this.p50.add(index, v));
            apply(values, 1, Number::longValue, null == index ? this.p95::add : v -> this.p95.add(index, v));
            apply(values, 2, Number::longValue, null == index ? this.p99::add : v -> this.p99.add(index, v));
        }

        @Override
        public void sort(int sort) {
            Map<Long, Object[]> valuesMap = this.valuesMap();
            super.sort(sort);
            this.p50 = new ArrayList<>();
            this.p95 = new ArrayList<>();
            this.p99 = new ArrayList<>();
            for (Long time : this.time) {
                Object[] values = Optional.ofNullable(valuesMap.get(time)).orElse(new Object[]{null, null, null});
                apply(values, 0, Number::longValue, this.p50::add);
                apply(values, 1, Number::longValue, this.p95::add);
                apply(values, 2, Number::longValue, this.p99::add);
            }
        }

        @Override
        protected Object[] values(int index) {
            return new Object[]{get(this.p50, index), get(this.p95, index), get(this.p99, index)};
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricErrorRate extends MetricBase {
        List<Double> errorRate;

        public MetricErrorRate() {
            super();
            this.errorRate = new ArrayList<>();
        }

        @Override
        public void add(Integer index, Long time, Object... values) {
            super.add(index, time);
            apply(values, 0, Number::doubleValue, null == index ? this.errorRate::add : v -> this.errorRate.add(index, v));
        }

        @Override
        public void sort(int sort) {
            Map<Long, Object[]> valuesMap = this.valuesMap();
            super.sort(sort);
            this.errorRate = new ArrayList<>();
            for (Long time : this.time) {
                Object[] values = Optional.ofNullable(valuesMap.get(time)).orElse(new Object[]{null});
                apply(values, 0, Number::doubleValue, this.errorRate::add);
            }
        }

        @Override
        protected Object[] values(int index) {
            return new Object[]{get(this.errorRate, index)};
        }
    }
}
