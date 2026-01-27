package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.commons.base.SortField;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
            long tsFrom, long tsEnd, TimeGranularity granularity,
            Function<Long, T> emptyGetter, Consumer<T> mapping) {
        tsFrom = granularity.fixTime(tsFrom);
        long step = 5L;
        if (granularity == TimeGranularity.MINUTE) {
            step = 60L;
        } else if (granularity == TimeGranularity.HOUR) {
            step = 60L * 60L;
            if (tsFrom % step != 0L) {
                tsFrom = tsFrom / step * step;
            }
            if (tsEnd % step != 0L) {
                tsEnd = (tsEnd / step + 1) * step;
            }
        }
        while (tsFrom <= tsEnd) {
            items.computeIfAbsent(tsFrom, k -> {
                T t = emptyGetter.apply(k);
                t.setTs(k);
                return t.empty();
            });
            tsFrom += step;
        }
        ArrayList<T> itemValues = new ArrayList<>(items.values());
//        for (int i = itemValues.size() - 1; i >= 0; i--) {
//            T item = itemValues.get(i);
//            if (item.isEmpty()) {
//               itemValues.remove(i) ;
//            } else {
//                break;
//            }
//        }
        itemValues.sort(Comparator.comparingLong(ValueBase.Item::getTs));
        itemValues.forEach(mapping);
        return itemValues;
    }

    public static <T> void sort(List<T> obj, QueryBase.SortInfo sortInfo, Class<T> tClass) {
        if (CollectionUtils.isEmpty(obj)) {
            return;
        }
        Field sortField = getSortFieldName(sortInfo, tClass);
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

    public static <T> Field getSortFieldName(QueryBase.SortInfo sortInfo, Class<T> tClass) {
        Map<String, Field> declaredFields = getAllFieldMap(tClass);
        return Optional.ofNullable(sortInfo)
                .map(e -> StringUtils.isNotBlank(e.getField()) ? e.getField() : null)
                .map(declaredFields::get)
                .orElse(findDefaultField(declaredFields.values()));
    }

    static <T> Double get(T obj, Field sortField) {
        try {
            if (sortField.get(obj) instanceof Number iNum) {
                return iNum.doubleValue();
            }
            return -1D;
        } catch (Exception e) {
            return -1D;
        }
    }

    static Field findDefaultField(Collection<Field> declaredFields) {
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

    public static Map<String, Field> getAllFieldMap(Class<?> clazz) {
        Map<String, Field> fieldMap = new HashMap<>();
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Field f : current.getDeclaredFields()) {
                f.setAccessible(true);
                fieldMap.putIfAbsent(f.getName(), f);
            }
            current = current.getSuperclass();
        }
        return fieldMap;
    }
}
