package com.tapdata.taskinspect;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.taskinspect.vo.TaskInspectCdcEvent;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 校验模式的实现接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/17 18:50 Create
 */
public interface IMode {

    default TaskInspectMode getMode() {
        return TaskInspectMode.CLOSE;
    }

    default void refresh(TaskInspectConfig config) throws InterruptedException {
    }

    /**
     * 需要在校验停止后还能正常调用
     *
     * @param event 增量事件
     */
    default void acceptCdcEvent(TaskInspectCdcEvent event) {
    }

    default void syncDelay(long delay) {
    }

    default void acceptCdcEvent(DataProcessorContext dataProcessorContext, TapdataEvent event) {
        if (event.getTapEvent() instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) event.getTapEvent();
            LinkedHashMap<String, Object> keys = getKeys(dataProcessorContext, recordEvent.getTableId(), recordEvent.getAfter());
            if (!keys.isEmpty()) {
                acceptCdcEvent(TaskInspectCdcEvent.create(
                    recordEvent.getReferenceTime(),
                    recordEvent.getTime(),
                    recordEvent.getTableId(),
                    keys
                ));
            }
        } else if (event.getTapEvent() instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) event.getTapEvent();
            LinkedHashMap<String, Object> keys = getKeys(dataProcessorContext, recordEvent.getTableId(), recordEvent.getAfter());
            if (!keys.isEmpty()) {
                acceptCdcEvent(TaskInspectCdcEvent.create(
                    recordEvent.getReferenceTime(),
                    recordEvent.getTime(),
                    recordEvent.getTableId(),
                    keys
                ));
            }
            keys = getKeys(dataProcessorContext, recordEvent.getTableId(), recordEvent.getBefore());
            if (!keys.isEmpty()) {
                acceptCdcEvent(TaskInspectCdcEvent.create(
                    recordEvent.getReferenceTime(),
                    recordEvent.getTime(),
                    recordEvent.getTableId(),
                    keys
                ));
            }
        } else if (event.getTapEvent() instanceof TapDeleteRecordEvent) {
            TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) event.getTapEvent();
            LinkedHashMap<String, Object> keys = getKeys(dataProcessorContext, recordEvent.getTableId(), recordEvent.getBefore());
            if (!keys.isEmpty()) {
                acceptCdcEvent(TaskInspectCdcEvent.create(
                    recordEvent.getReferenceTime(),
                    recordEvent.getTime(),
                    recordEvent.getTableId(),
                    keys
                ));
            }
        }
    }

    default LinkedHashMap<String, Object> getKeys(DataProcessorContext dataProcessorContext, String tableId, Map<String, Object> data) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        Optional.ofNullable(dataProcessorContext.getTapTableMap())
            .map(tapTableMap -> tapTableMap.get(tableId))
            .map(tapTable -> tapTable.primaryKeys(true))
            .ifPresent(keys -> {
                for (String k : keys) {
                    result.put(k, data.get(k));
                }
            });
        return result;
    }

    default boolean stop() {
        return true;
    }

}
