package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class HazelcastRenameTableProcessorNode extends HazelcastProcessorBaseNode {

    /**
     * key: 源表名
     */
    private Map<String, TableRenameTableInfo> tableNameMappingMap;
    private final Map<String, String> innerTableCacheMap = new ConcurrentHashMap<>(); // Used to speed up the name change operation

    public HazelcastRenameTableProcessorNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        initTableNameMapping();
    }

    private void initTableNameMapping() {
        TableRenameProcessNode tableRenameProcessNode = (TableRenameProcessNode) getNode();
        this.tableNameMappingMap = tableRenameProcessNode.previousMap();
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();

        if (tapEvent instanceof TapBaseEvent) {
            String newTableName = getTgtTableNameFromTapEvent(tapEvent);
            ((TapBaseEvent) tapEvent).setTableId(newTableName);
        }
        consumer.accept(tapdataEvent, getProcessResult(TapEventUtil.getTableId(tapEvent)));
    }

    @Override
    public String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
        String tableId = TapEventUtil.getTableId(tapEvent);
        TableRenameProcessNode tableRenameProcessNode = (TableRenameProcessNode) getNode();
        if (tapEvent instanceof TapRenameTableEvent) {
            String newTableId = tableRenameProcessNode.convertTableName(tableNameMappingMap, tableId, true);
            innerTableCacheMap.put(tableId, newTableId);
            return newTableId;
        } else if (tapEvent instanceof TapDropTableEvent) {
            String newTableId = innerTableCacheMap.remove(tableId);
            if (null == newTableId) {
                newTableId = tableRenameProcessNode.convertTableName(tableNameMappingMap, tableId, false);
            }
            return newTableId;
        } else {
            return innerTableCacheMap.computeIfAbsent(tableId, id -> tableRenameProcessNode.convertTableName(tableNameMappingMap, tableId, false));
        }
    }

    @Override
    public boolean needTransformValue() {
        return false;
    }
}
