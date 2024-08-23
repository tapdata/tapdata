package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.ArrayModel;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnwindModel;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind.EventHandel;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind.UnWindNodeUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * @author GavinXiao
 * @description HazelcastUnwindProcessNode create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
 **/
public class HazelcastUnwindProcessNode extends HazelcastProcessorBaseNode {
    private static final Logger logger = LogManager.getLogger(HazelcastUnwindProcessNode.class);
    public static final String TAG = HazelcastUnwindProcessNode.class.getSimpleName();
    UnwindProcessNode node;

    @SneakyThrows
    public HazelcastUnwindProcessNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        Node<?> node = getNode();
        if (node instanceof UnwindProcessNode) {
            this.node = (UnwindProcessNode) node;
        }
    }


    @SneakyThrows
    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        String tableName = TapEventUtil.getTableId(tapEvent);
        ProcessResult processResult = getProcessResult(tableName);

        if (!(tapEvent instanceof TapRecordEvent)) {
            consumer.accept(tapdataEvent, processResult);
            return;
        }

        List<TapEvent> eventList = EventHandel.getHandelResult(node, tapdataEvent.getTapEvent());
        if (CollectionUtils.isNotEmpty(eventList)) {
            for (TapEvent e : eventList) {
                TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
                cloneTapdataEvent.setTapEvent(e);
                consumer.accept(cloneTapdataEvent, processResult);
            }
        }
    }

    @Override
    protected void doClose() throws TapCodeException {
        super.doClose();
        EventHandel.close();
    }

    @Override
    protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
        tapdataEvent.setTransformToTapValueResult(null);
    }
    @Override
    public boolean needCopyBatchEventWrapper() {
        return true;
    }
}
