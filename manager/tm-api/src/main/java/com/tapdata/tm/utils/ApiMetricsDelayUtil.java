package com.tapdata.tm.utils;

import lombok.Getter;
import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.DoubleConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/29 17:29 Create
 * @description
 */
public final class ApiMetricsDelayUtil {
    public static final String KEY = "k";
    public static final String VALUE = "v";
    public static final Logger log = org.slf4j.LoggerFactory.getLogger(ApiMetricsDelayUtil.class);

    private ApiMetricsDelayUtil() {

    }

    @SafeVarargs
    public static List<Map<String, Number>> merge(List<Map<String, Number>>... delayList) {
        List<List<Map<String, Number>>> delayLists = new ArrayList<>(Arrays.asList(delayList));
        return merge(delayLists);
    }

    public static List<Map<String, Number>> merge(List<List<Map<String, Number>>> delayList) {
        if (CollectionUtils.isEmpty(delayList)) {
            return new ArrayList<>();
        }
        Map<Double, Integer> merged = new HashMap<>();
        for (List<Map<String, Number>> list : delayList) {
            if (null == list) {
                continue;
            }
            for (Map<String, Number> m : list) {
                if (null == m) {
                    continue;
                }
                Number k = parseKey(m.get(KEY));
                if (null != k) {
                    int iV = Optional.ofNullable(merged.get(k.doubleValue())).orElse(0);
                    int iVal = Optional.ofNullable(m.get(VALUE)).map(Number::intValue).orElse(0);
                    int sum = iV + iVal;
                    if (sum > 0) {
                        merged.put(k.doubleValue(), sum);
                    }
                }
            }
        }
        List<Map<String, Number>> result = new ArrayList<>();
        merged.forEach((k, v) -> {
            Map<String, Number> iMap = new HashMap<>();
            iMap.put(KEY, k);
            iMap.put(VALUE, v);
            result.add(iMap);
        });
        return result;
    }

    @Getter
    public static class Sum {
        long count;
        double total;
    }

    public static Sum sum(List<Map<String, Number>> delayList) {
        Sum sumOf = new Sum();
        if (CollectionUtils.isEmpty(delayList)) {
            return sumOf;
        }
        double sum = 0L;
        long count = 0L;
        for (Map<String, Number> m : delayList) {
            double iKey = Optional.ofNullable(m.get(KEY)).map(Number::doubleValue).orElse(0D);
            Integer iValue = Optional.ofNullable(m.get(VALUE)).map(Number::intValue).orElse(0);
            sum += iKey * iValue.longValue();
            count += iValue.longValue();
        }
        sumOf.count = count;
        sumOf.total = sum;
        return sumOf;
    }

    public static Double p95(List<Map<String, Number>> delayList, long total) {
        if (total <= 20 || delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.95D);
    }

    public static Double p99(List<Map<String, Number>> delayList, long total) {
        if (total <= 10 || delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.99D);
    }

    public static Double p50(List<Map<String, Number>> delayList, long total) {
        if (delayList.size() < 2) {
            return null;
        }
        return p(delayList, total, 0.5D);
    }

    public static void readMaxAndMin(List<Map<String, Number>> delayList, DoubleConsumer max, DoubleConsumer min) {
        Double minValue = null;
        Double maxValue = null;
        for (Map<String, Number> item : delayList) {
            Number iKey = item.get(KEY);
            if (null == iKey) {
                continue;
            }
            double reqDelay = iKey.doubleValue();
            if (null == minValue) minValue = reqDelay;
            else minValue = Math.min(reqDelay, minValue);
            if (null == maxValue) maxValue = reqDelay;
            else maxValue = Math.max(reqDelay, maxValue);
        }
        if (null != min) {
            min.accept(Optional.ofNullable(minValue).orElse(0D));
        }
        if (null != max) {
            max.accept(Optional.ofNullable(maxValue).orElse(0D));
        }
    }

    public static Double p(List<Map<String, Number>> delayList, long total, double p) {
        int target = ((Double) Math.ceil(total * p)).intValue();
        int sum = 0;
        Double pVal = null;
        delayList.sort(Comparator.comparing(m -> Optional.ofNullable(m.get(KEY)).map(Number::longValue).orElse(0L)));
        for (Map<String, Number> m : delayList) {
            Number iKey = m.get(KEY);
            if (null == iKey) {
                continue;
            }
            int iVal = Optional.ofNullable(m.get(VALUE)).map(Number::intValue).orElse(0);
            if (sum >= target) {
                return iKey.doubleValue();
            }
            sum += iVal;
            pVal = iKey.doubleValue();

            if (sum >= target) {
                return pVal;
            }
        }
        return sum >= target ? pVal : null;
    }

    public static Number parseKey(Number key) {
        if (key instanceof Double iDouble) {
            if (iDouble >= 1D) {
                return iDouble.longValue();
            }
            if (iDouble <= 0D) {
                return 0L;
            }
            BigDecimal bigDecimal = BigDecimal.valueOf(iDouble).setScale(1, RoundingMode.HALF_DOWN);
            return bigDecimal.stripTrailingZeros().doubleValue();
        } else {
            return key;
        }
    }

    public static List<Map<String, Number>> addDelay(List<Map<String, Number>> delay, Number value) {
        if (null == delay) {
            delay = new ArrayList<>();
        }
        value = parseKey(value);
        for (int i = 0; i < delay.size(); i++) {
            Map<String, Number> item = delay.get(i);
            if (Objects.equals(item.get(KEY), value)) {
                item.put(VALUE, Optional.ofNullable(item.get(VALUE)).map(Number::intValue).orElse(0) + 1);
                return delay;
            }
        }
        Map<String, Number> iMap = new HashMap<>();
        iMap.put(KEY, value);
        iMap.put(VALUE, 1);
        delay.add(iMap);
        return delay;
    }

    public static Map<String, Number> toMap(Long key, Integer value) {
        Map<String, Number> iMap = new HashMap<>();
        iMap.put(KEY, key);
        iMap.put(VALUE, value);
        return iMap;
    }
}
