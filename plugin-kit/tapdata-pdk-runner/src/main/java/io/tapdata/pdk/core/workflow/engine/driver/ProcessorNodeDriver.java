package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.pdk.apis.functions.processor.ProcessRecordFunction;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.api.ProcessorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.pdk.core.utils.queue.ListHandler;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;

import java.util.ArrayList;
import java.util.List;

public class ProcessorNodeDriver extends Driver implements ListHandler<List<TapEvent>> {
    private static final String TAG = ProcessorNodeDriver.class.getSimpleName();

    private ProcessorNode processorNode;
    private SingleThreadBlockingQueue<List<TapEvent>> queue;

    @Override
    public void execute(List<List<TapEvent>> list) throws Throwable {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();

        for(List<TapEvent> events : list) {
            List<TapEvent> recordEvents = new ArrayList<>();
            for (TapEvent event : events) {
                if(event instanceof TapInsertRecordEvent) {
                    recordEvents.add(event);
                }
            }
            ProcessRecordFunction processRecordFunction = processorNode.getProcessorFunctions().getProcessRecordFunction();
            if(processRecordFunction != null) {
                TapLogger.debug(TAG, "Process {} of record events, {}", recordEvents.size(), LoggerUtils.processorNodeMessage(processorNode));
                pdkInvocationMonitor.invokePDKMethod(processorNode, PDKMethod.PROCESSOR_PROCESS_RECORD, () -> {
                    processRecordFunction.process(processorNode.getProcessorContext(), recordEvents, (event) -> {
                        TapLogger.debug(TAG, "Processed {} of record events, {}", recordEvents.size(), LoggerUtils.processorNodeMessage(processorNode));
                        //TODO not sure how to do this for processor, do it later.
//                        offer(recordEvents);
//                        offer(events, (theEvents) -> PDKIntegration.filterEvents(sourceNode, theEvents));
                    });
                }, "insert " + LoggerUtils.processorNodeMessage(processorNode), TAG);
            }
        }
    }

    public ProcessorNode getProcessorNode() {
        return processorNode;
    }

    public void setProcessorNode(ProcessorNode processorNode) {
        this.processorNode = processorNode;
    }

    public SingleThreadBlockingQueue<List<TapEvent>> getQueue() {
        return queue;
    }

    public void setQueue(SingleThreadBlockingQueue<List<TapEvent>> queue) {
        this.queue = queue;
    }
}
