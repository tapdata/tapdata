package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.sampler.SpeedSampler;

import java.util.Optional;

/**
 * @author Dexter
 */
public class ProcessorNodeSampleHandler extends AbstractNodeSampleHandler {
    public ProcessorNodeSampleHandler(TaskDto task, Node<?> node) {
        super(task, node);
    }

    public void handleProcessStart(HandlerUtil.EventTypeRecorder recorder) {
        Optional.ofNullable(inputInsertCounter).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounter).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounter).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputDdlCounter).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputOthersCounter).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputSpeed).ifPresent(SpeedSampler::add);
    }

    public void handleProcessAccept(HandlerUtil.EventTypeRecorder recorder) {
        Optional.ofNullable(outputInsertCounter).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(outputUpdateCounter).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(outputDeleteCounter).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(outputDdlCounter).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(outputOthersCounter).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(outputSpeed).ifPresent(speed -> speed.add(recorder.getTotal()));

        Optional.ofNullable(currentEventTimestamp).ifPresent(number -> number.setValue(recorder.getNewestEventTimestamp()));
        Optional.ofNullable(replicateLag).ifPresent(counter -> {
            if (null != recorder.getReplicateLagTotal()) {
                counter.setValue(recorder.getTotal(), recorder.getReplicateLagTotal());
            }
        });
    }

    public void handleProcessEnd(Long startAt, Long endAt, long total) {
        Optional.ofNullable(timeCostAverage).ifPresent(average ->
                average.add(total, endAt - startAt));
    }
}
