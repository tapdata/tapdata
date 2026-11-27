package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.dql.classifier.DqlFailedStage;

/**
 * Carries a processor failure to the skip-error aspect task.
 *
 * <p>The aspect is intentionally separate from {@link ProcessorFunctionAspect}:
 * processor execution must be able to inspect the final failure handling result
 * and only suppress the original error after a DLQ event has been accepted.</p>
 */
public class SkipErrorProcessAspect extends ProcessorNodeAspect<SkipErrorProcessAspect> {
    private TapdataEvent inputEvent;
    private Throwable error;
    private DqlFailedStage processStage;
    private String nodeId;
    private String nodeName;

    public SkipErrorProcessAspect inputEvent(TapdataEvent inputEvent) {
        this.inputEvent = inputEvent;
        return this;
    }

    public SkipErrorProcessAspect error(Throwable error) {
        this.error = error;
        return this;
    }

    public SkipErrorProcessAspect processStage(DqlFailedStage processStage) {
        this.processStage = processStage;
        return this;
    }

    public SkipErrorProcessAspect nodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public SkipErrorProcessAspect nodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    public TapdataEvent getInputEvent() {
        return inputEvent;
    }

    public void setInputEvent(TapdataEvent inputEvent) {
        this.inputEvent = inputEvent;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public DqlFailedStage getProcessStage() {
        return processStage;
    }

    public void setProcessStage(DqlFailedStage processStage) {
        this.processStage = processStage;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
}
