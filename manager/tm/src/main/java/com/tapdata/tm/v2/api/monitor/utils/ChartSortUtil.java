package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.commons.base.SortField;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/4 14:06 Create
 * @description
 */
public final class ChartSortUtil {

    private ChartSortUtil() {

    }

    public static <T extends ValueBase.Item> List<T> fixAndSort(
            Map<Long, T> items,
            long tsFrom, long tsEnd, int granularity,
            Function<Long, T> emptyGetter, Consumer<T> mapping) {
        long step = 5L;
        if (granularity == 1) {
            step = 60L;
        } else if (granularity == 2) {
            step = 60L * 60L;
            if (tsFrom % step != 0L) {
                tsFrom = tsFrom / step * step;
            }
            if (tsEnd % step != 0L) {
                tsEnd = (tsEnd / step + 1) * step;
            }
        }
        while (tsFrom < tsEnd) {
            items.computeIfAbsent(tsFrom, emptyGetter);
            tsFrom += step;
        }
        ArrayList<T> itemValues = new ArrayList<>(items.values());
        itemValues.sort(Comparator.comparingLong(ValueBase.Item::getTs));
        itemValues.forEach(mapping);
        return itemValues;
    }

    public static <T> void sort(List<T> obj, QueryBase.SortInfo sortInfo, Class<T> tClass) {
        if (CollectionUtils.isEmpty(obj)) {
            return;
        }
        Field[] declaredFields = tClass.getDeclaredFields();
        Field sortField = Optional.ofNullable(sortInfo)
                .map(e -> StringUtils.isNotBlank(e.getField()) ? e.getField() : null)
                .map(e -> findField(declaredFields, e))
                .orElse(findDefaultField(declaredFields));
        if (null == sortField) {
            return;
        }
        sortField.setAccessible(true);
        Comparator<Object> comparing = Comparator.comparing(o -> get(o, sortField));
        if (null == sortInfo || StringUtils.isBlank(sortInfo.getOrder()) || "DESC".equalsIgnoreCase(sortInfo.getOrder())) {
            comparing = comparing.reversed();
        }
        obj.sort(comparing);
    }

    static <T> Double get(T obj, Field sortField) {
        try {
            if (sortField.get(obj) instanceof Number iNum) {
                return iNum.doubleValue();
            }
            return 0D;
        } catch (Exception e) {
            return 0D;
        }
    }

    static Field findDefaultField(Field[] declaredFields) {
        for (Field declaredField : declaredFields) {
            SortField annotation = declaredField.getAnnotation(SortField.class);
            if (null == annotation) {
                continue;
            }
            if (annotation.normal()) {
                return declaredField;
            }
        }
        return null;
    }

    static Field findField(Field[] declaredFields, String name) {
        for (Field declaredField : declaredFields) {
            SortField annotation = declaredField.getAnnotation(SortField.class);
            if (null == annotation) {
                continue;
            }
            String[] names = annotation.name();
            if (null == names || names.length == 0 && StringUtils.equalsIgnoreCase(name, declaredField.getName())) {
                return declaredField;
            }
            for (String aila : names) {
                if (StringUtils.equalsIgnoreCase(name, aila)) {
                    return declaredField;
                }
            }
        }
        return null;
    }
}
