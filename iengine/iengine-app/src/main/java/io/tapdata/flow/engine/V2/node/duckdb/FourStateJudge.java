package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 四态判断器
 * 根据 before/after 数据判断 INSERT/UPDATE/DELETE/SKIP 操作
 * 输出标准 TapdataEvent 事件
 */
public class FourStateJudge {

    private static final Logger logger = LoggerFactory.getLogger(FourStateJudge.class);

    private final String tableId;
    private final String wideTablePrimaryKey;

    public FourStateJudge(String tableId, String wideTablePrimaryKey) {
        this.tableId = tableId;
        this.wideTablePrimaryKey = wideTablePrimaryKey;
    }

    /**
     * 四态判断
     * @param beforePks before SQL 返回的主键集合
     * @param afterData after SQL 返回的完整数据
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> judge(Set<Object> beforePks, List<Map<String, Object>> afterData) {
        List<TapdataEvent> events = new ArrayList<>();
        List<Map<String, Object>> afterDataList = afterData != null ? afterData : new ArrayList<>();
        Set<Object> afterPks = extractPrimaryKeys(afterDataList);
        // 有旧无新 → DELETE
        for (Object pk : beforePks) {
            if (!afterPks.contains(pk)) {
                Map<String, Object> before = new HashMap<>();
                before.put(wideTablePrimaryKey, pk);
                TapDeleteRecordEvent deleteEvent = TapDeleteRecordEvent.create()
                        .table(tableId)
                        .referenceTime(System.currentTimeMillis())
                        .before(before);
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setTapEvent(deleteEvent);
                tapdataEvent.setSyncStage(SyncStage.CDC);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: DELETE pk={}", pk);
            }
        }
        // 无旧有新 → INSERT / 新旧都有 → UPDATE
        for (Map<String, Object> row : afterDataList) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk == null) {
                logger.warn("Wide table primary key '{}' not found in row: {}", wideTablePrimaryKey, row);
                continue;
            }
            if (beforePks.contains(pk)) {
                TapUpdateRecordEvent updateEvent = TapUpdateRecordEvent.create()
                        .table(tableId)
                        .referenceTime(System.currentTimeMillis())
                        .after(row);
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setTapEvent(updateEvent);
                tapdataEvent.setSyncStage(SyncStage.CDC);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: UPDATE pk={}", pk);
            } else {
                TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
                        .referenceTime(System.currentTimeMillis())
                        .table(tableId)
                        .after(row);
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setSyncStage(SyncStage.CDC);
                tapdataEvent.setTapEvent(insertEvent);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: INSERT pk={}", pk);
            }
        }
        logger.debug("Four-state judge result: {} events (beforePks={}, afterPks={})",
                events.size(), beforePks.size(), afterPks.size());

        return events;
    }

    private Set<Object> extractPrimaryKeys(List<Map<String, Object>> data) {
        Set<Object> pks = new HashSet<>();
        for (Map<String, Object> row : data) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk != null) {
                pks.add(pk);
            }
        }
        return pks;
    }
}
