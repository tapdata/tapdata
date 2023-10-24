package io.tapdata.entity.event.control;

public interface PatrolListener {
    void patrol(String nodeId, int state) throws Throwable;
}
