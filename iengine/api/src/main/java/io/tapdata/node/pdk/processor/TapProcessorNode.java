package io.tapdata.node.pdk.processor;

import com.hazelcast.jet.core.Processor.*;
import com.tapdata.entity.TapdataShareLogEvent;
import io.tapdata.entity.TapProcessorNodeContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapProcessorContext;

import java.util.List;

public interface TapProcessorNode {
    void doInit(Context context, TapProcessorNodeContext tapProcessorNodeContext);
    void processEvents(List<TapEvent> tapEvents);
    void doClose();
    void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents);
}
