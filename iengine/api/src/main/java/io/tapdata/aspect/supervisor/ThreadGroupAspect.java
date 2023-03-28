package io.tapdata.aspect.supervisor;

import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.DataNodeAspect;

public abstract class ThreadGroupAspect<T extends DataNodeAspect<?>> extends DataNodeAspect<T> {
    protected ThreadGroup threadGroup;
    protected String associateId;
    protected Node<?> node;

    public String getAssociateId() {
        return associateId;
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public Node<?> getNode() {
        return node;
    }

    public ThreadGroupAspect(Node<?> node, String associateId, ThreadGroup threadGroup) {
        super();
        this.threadGroup = threadGroup;
        this.associateId = associateId;
        this.node = node;
    }
    public ThreadGroupAspect(String associateId, ThreadGroup threadGroup) {
        super();
        this.threadGroup = threadGroup;
        this.associateId = associateId;
    }
}
