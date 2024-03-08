package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.AddDateFieldProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateAddDateFieldProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;
import java.util.function.BiConsumer;

public class HazelcastAddDateFieldProcessNode extends HazelcastProcessorBaseNode{
    private String dateFieldName;
    public HazelcastAddDateFieldProcessNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        Node node = getNode();
        if (node instanceof AddDateFieldProcessorNode) {
            dateFieldName = ((AddDateFieldProcessorNode) node).getDateFieldName();
        } else {
            dateFieldName = ((MigrateAddDateFieldProcessorNode) node).getDateFieldName();
        }
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        String tableName = TapEventUtil.getTableId(tapEvent);
        ProcessResult processResult = getProcessResult(tableName);
        if (!(tapEvent instanceof TapRecordEvent)) {
            consumer.accept(tapdataEvent, processResult);
            return;
        }
        Map<String, Object> record = TapEventUtil.getAfter(tapEvent);
        if (MapUtils.isEmpty(record) && MapUtils.isNotEmpty(TapEventUtil.getBefore(tapEvent))) {
            record = TapEventUtil.getBefore(tapEvent);
        }
        if (!record.containsKey(dateFieldName)) {
            record.put(dateFieldName, new DateTime(System.currentTimeMillis()));
        }
        consumer.accept(tapdataEvent, processResult);
    }
}
