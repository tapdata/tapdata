package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
}
