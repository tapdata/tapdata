package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.function.BiConsumer;

public class HazelcastMigrateUnionProcessorNode extends HazelcastProcessorBaseNode{
    private String tableName;

    public HazelcastMigrateUnionProcessorNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        if((getNode() instanceof MigrateUnionProcessorNode)){
            MigrateUnionProcessorNode unionProcessorNode = (MigrateUnionProcessorNode)getNode();
            this.tableName = unionProcessorNode.getTableName();
        }
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        if(tapEvent instanceof TapCreateTableEvent){
            tapEvent.addInfo(SKIP_TARGET_CREATE_TABLE_INFO_KEY, true);
            obsLogger.info("The UnionProcessor detected a create table event. The target will skip physical table creation while continuing model updates, tableName: {}", ((TapCreateTableEvent) tapEvent).getTableId());
            consumer.accept(tapdataEvent, getProcessResult(TapEventUtil.getTableId(tapEvent)));
            return;
        }
        if (tapEvent instanceof TapBaseEvent) {
            ((TapBaseEvent) tapEvent).setTableId(tableName);
        }
        consumer.accept(tapdataEvent, getProcessResult(TapEventUtil.getTableId(tapEvent)));
    }

    @Override
    public boolean needTransformValue() {
        return false;
    }
}
