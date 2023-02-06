package io.tapdata.coding.service.loader;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;

import java.util.*;

import static io.tapdata.coding.enums.TapEventTypes.CREATED_EVENT;
import static io.tapdata.coding.enums.TapEventTypes.UPDATE_EVENT;

public class OverlayQueryEventDifferentiator {
    //记录的主键以及记录的HashCode:作用就是用来确定新增事件或者修改事件，
    // 如果这个map中不存在某个主键的hashCode，那么表示新增事件
    // 如果存在这个主键，对吧map中的hashCode 与 目前记录的hashCode,是否相等，不相等表示修改过，否则不做事件处理
    public Map<Integer, Integer> recordHashMap = new HashMap<>();

    //用来记录上一轮事件完成后的全部记录的主键
    //如果某个记录的主键在这个Set中存在，并且在recordHashMap中存在，那么表示上一轮操作和本论操作都存在这个记录，否则表示这个记录被删除了
    public Set<Integer> currentBatchSet = new HashSet<>();

    public String createOrUpdateEvent(Integer code, Integer hash) {
        String event = recordHashMap.containsKey(code) ?
                (!hash.equals(recordHashMap.get(code)) ? UPDATE_EVENT : null)
                : CREATED_EVENT;
        if (Checker.isNotEmpty(event)) {
            currentBatchSet.add(code);
            recordHashMap.put(code, hash);
            return event;
        }
        return "NOT_EVENT";
    }

    public List<TapEvent> delEvent(String tableName, String primaryKeyName) {
        List<TapEvent> events = new ArrayList<>();
        recordHashMap.forEach((iterationCode, hash) -> {
            if (!currentBatchSet.contains(iterationCode)) {
                events.add(TapSimplify.deleteDMLEvent(
                        new HashMap<String, Object>() {{
                            put(primaryKeyName, iterationCode);
                        }}
                        , tableName
                ).referenceTime(System.currentTimeMillis()));
            }
        });
        currentBatchSet.clear();
        return events;
    }
}
