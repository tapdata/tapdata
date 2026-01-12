package com.tapdata.tm.v2.api.monitor.utils;

import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/29 17:29 Create
 * @description
 */
public final class ApiMetricsDelayUtil {
    public static final Logger log = org.slf4j.LoggerFactory.getLogger(ApiMetricsDelayUtil.class);

    private ApiMetricsDelayUtil() {

    }

    public static List<Map<Long, Integer>> fixDelayAsMap(List<?> delay) {
        if (CollectionUtils.isEmpty(delay)) {
            delay = new ArrayList<>();
        }
        List<Map<Long, Integer>> result = new ArrayList<>();
        for (Object o : delay) {
            if (o instanceof Map<?, ?> iMap) {
                iMap.forEach((k, v) -> each(k, v, result));
            } else if (o instanceof Number iNumber) {
                result.add(toMap(iNumber.longValue(), 1));
            }
        }
        return result;
    }

    static void each(Object k, Object v, List<Map<Long, Integer>> result) {
        Map<Long, Integer> yMap = new HashMap<>();
        if (k instanceof Number iKey && v instanceof Number iValue) {
            yMap.put(iKey.longValue(), iValue.intValue());
            result.add(yMap);
        } else if (k instanceof String iKey && v instanceof Number iValue) {
            try {
                yMap.put(Long.parseLong(iKey), iValue.intValue());
                result.add(yMap);
            } catch (NumberFormatException e) {
                log.warn(e.getMessage());
            }
        } else if (k instanceof String iKey && v instanceof String iValue) {
            try {
                yMap.put(Long.parseLong(iKey), Integer.parseInt(iValue));
                result.add(yMap);
            } catch (NumberFormatException e) {
                log.warn(e.getMessage());
            }
        } else if (k instanceof Number iKey && v instanceof String iValue) {
            try {
                yMap.put(iKey.longValue(), Integer.parseInt(iValue));
                result.add(yMap);
            } catch (NumberFormatException e) {
                log.warn(e.getMessage());
            }
        } else {
            log.warn("Unknown support entry: {} : {}", k, v);
        }
    }

    @SafeVarargs
    public static List<Map<Long, Integer>> merge(List<Map<Long, Integer>>... delayList) {
        if (null == delayList) {
            return new ArrayList<>();
        }
        Map<Long, Integer> merged = new HashMap<>();
        for (List<Map<Long, Integer>> list : delayList) {
            if (null == list) {
                continue;
            }
            for (Map<Long, Integer> m : list) {
                if (null == m) {
                    continue;
                }
                for (Map.Entry<Long, Integer> e : m.entrySet()) {
                    if (null == e.getKey()) {
                        continue;
                    }
                    merged.merge(e.getKey(), Optional.ofNullable(e.getValue()).orElse(0), Integer::sum);
                }
            }
        }
        return new ArrayList<>(merged.entrySet()
                .stream()
                .map(e -> toMap(e.getKey(), e.getValue()))
                .toList());
    }

    public static Long sum(List<?> delayList) {
        if (CollectionUtils.isEmpty(delayList)) {
            return 0L;
        }
        long sum = 0L;
        for (Map<Long, Integer> m : fixDelayAsMap(delayList)) {
            for (Map.Entry<Long, Integer> e : m.entrySet()) {
                Long iKey = e.getKey();
                Integer iValue = e.getValue();
                sum += iKey * iValue.longValue();
            }
        }
        return sum;
    }

    public static Long p95(List<Map<Long, Integer>> delayList, long total) {
        if (total <= 20 || delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.95D);
    }

    public static Long p99(List<Map<Long, Integer>> delayList, long total) {
        if (total <= 10 || delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.99D);
    }

    public static Long p50(List<Map<Long, Integer>> delayList, long total) {
        if (delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.5D);
    }

    public static void readMaxAndMin(List<Map<Long, Integer>> delayList, LongConsumer max, LongConsumer min) {
        Set<Long> delays = new HashSet<>();
        for (Map<Long, Integer> item : delayList) {
            delays.addAll(item.keySet());
        }
        delays.stream().mapToLong(Long::longValue).max().ifPresent(max);
        delays.stream().mapToLong(Long::longValue).min().ifPresent(min);
    }

    static Long p(List<Map<Long, Integer>> delayList, long total, double p) {
        int target = ((Double) Math.ceil(total * p)).intValue();
        int sum = 0;
        Long pVal = null;
        delayList.sort(Comparator.comparing(m -> m.keySet().stream().findFirst().orElse(0L)));
        for (Map<Long, Integer> m : delayList) {
            for (Map.Entry<Long, Integer> e : m.entrySet()) {
                if (sum >= target) {
                    return e.getKey();
                }
                sum += e.getValue();
                pVal = e.getKey();
            }
            if (sum >= target) {
                pVal = m.keySet().stream().findFirst().orElse(null);
                if (null != pVal) {
                    return pVal;
                }
            }
        }
        return sum >= target ? pVal : null;
    }

    public static <T> List<T> addDelay(List<T> delay, long value) {
        if (null == delay) {
            delay = new ArrayList<>();
        }
        for (int i = 0; i < delay.size(); i++) {
            Object item = delay.get(i);
            if (item instanceof Map<?, ?> iMap) {
                Map<Object, Integer> typedMap = (Map<Object, Integer>) iMap;
                if (typedMap.containsKey(value)) {
                    typedMap.computeIfPresent(value, (k, v) -> v + 1);
                    return delay;
                }
            } else if (item instanceof Number iNumber && value == iNumber.longValue()) {
                ((List<Object>) delay).set(i, toMap(value, 2));
                return delay;
            }
        }
        ((List<Object>) delay).add(toMap(value, 1));
        return delay;
    }

    public static <K, V> Map<K, V> toMap(K key, V value) {
        Map<K, V> iMap = new HashMap<>();
        iMap.put(key, value);
        return iMap;
    }
}
