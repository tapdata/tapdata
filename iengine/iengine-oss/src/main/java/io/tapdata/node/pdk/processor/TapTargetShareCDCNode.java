package io.tapdata.node.pdk.processor;

import com.hazelcast.jet.core.Processor.*;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.TapProcessorNodeContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.exception.TapOssNonsupportFunctionException;
import io.tapdata.pdk.apis.context.TapProcessorContext;

import java.util.List;

public class TapTargetShareCDCNode implements TapProcessorNode{

    public TapTargetShareCDCNode(DataProcessorContext dataProcessorContext) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void doInit(Context context, TapProcessorNodeContext tapProcessorNodeContext) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void processEvents(List<TapEvent> tapEvents) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void doClose() {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
        throw new TapOssNonsupportFunctionException();
    }
}
