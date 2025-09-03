package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.vo.WorkerCallData;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface Compress {

    Long compressTime(WorkerCallEntity e);

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
        return result;
    }

    default  void fixTime(List<MetricDataBase> result, MockItem<? extends MetricDataBase> mockItem) {
        List<MetricDataBase> addItem = new ArrayList<>();
        long start = result.get(0).getTime();
        for (MetricDataBase metricDataBase : result) {
            while (metricDataBase.getTime() < start) {
                addItem.add(mockItem.mock(start));
                start = plus(start);
            }
        }
        if (!addItem.isEmpty()) {
            result.addAll(addItem);
        }
        result.sort(Comparator.comparing(MetricDataBase::getTime));
    }

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
