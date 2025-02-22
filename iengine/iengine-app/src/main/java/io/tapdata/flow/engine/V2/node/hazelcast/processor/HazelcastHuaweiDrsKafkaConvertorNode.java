package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.HuaweiDrsKafkaConvertorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.huawei.drs.kafka.ISerialization;

import java.util.function.BiConsumer;

/**
 * 华为 DRS Kafka 消息转换器 - 事件处理
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/19 11:40 Create
 */
public class HazelcastHuaweiDrsKafkaConvertorNode extends HazelcastProcessorBaseNode {

    private ISerialization serialization = null;

    public HazelcastHuaweiDrsKafkaConvertorNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        HuaweiDrsKafkaConvertorNode node = (HuaweiDrsKafkaConvertorNode) getNode();
        serialization = ISerialization.create(node.getStoreType(), node.getFromDBType());
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

        serialization.process(tapdataEvent, consumer, processResult);
    }
}
