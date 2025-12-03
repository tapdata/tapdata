package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class FixedSizeQueue<E extends Number> {
    private final int capacity;
    private final Deque<E> deque = new LinkedList<>();
    private final double stableThreshold;

    public FixedSizeQueue(int capacity, double stableThreshold) {
        if (capacity <= 0) {
            this.capacity = 10;
        } else {
            this.capacity = capacity;
        }
        if (stableThreshold <= 0 || stableThreshold >= 100D) {
            stableThreshold = 5D;
        }
        this.stableThreshold = stableThreshold;
    }

    public E push(E value) {
        if (null == value) {
            return null;
        }
        double item = value.doubleValue();
        if (deque.size() < capacity) {
            deque.addLast(value);
            return value;
        }
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double total = 0d;
        Map<E, Integer> hotMap = new HashMap<>();
        for (E v : deque) {
            min = Math.min(min, v.doubleValue());
            max = Math.max(max, v.doubleValue());
            total += v.doubleValue();
            hotMap.putIfAbsent(v, 0);
            hotMap.put(v, hotMap.get(v) + 1);
        }
        double range = max - min;
        double avg = total / deque.size();
        boolean stable = (range <= parse(Math.abs(avg) + stableThreshold, value).doubleValue()) && (range > parse(Math.abs(avg) - stableThreshold, value).doubleValue());
        E result;
        if (stable) {
            if (item >= min && item <= max) {
                AtomicReference<E> itemValue = new AtomicReference<>();
                hotMap.entrySet().stream()
                        .filter(e -> e.getValue() > 1)
                        .max(Map.Entry.comparingByValue())
                        .ifPresent(v -> itemValue.set(v.getKey()));
                result = Optional.ofNullable(itemValue.get()).orElse(value);
            } else {
                result = value;
            }
        } else {
            result = value;
        }
        deque.pollFirst();
        deque.addLast(value);
        return result;
    }

    E parse(double value, E item) {
        Number num;
        if (item instanceof Short) {
            num = ((Number) value).shortValue();
        } else if (item instanceof Integer) {
            num = ((Number) value).intValue();
        } else if (item instanceof Float) {
            num = ((Number) value).floatValue();
        } else if (item instanceof Long) {
            num = ((Number) value).longValue();
        } else {
            num = value;
        }
        return (E) num;
    }

    public E poll() {
        return deque.pollFirst();
    }

    public int size() {
        return deque.size();
    }

    @Override
    public String toString() {
        return deque.toString();
    }
}
