package io.tapdata.aspect.supervisor;

import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;

public class DisposableThreadGroupAspect<T extends DisposableThreadGroupBase> extends ThreadGroupAspect<DisposableThreadGroupAspect<T>> {
    boolean hasRelease;
    T entity;
    public DisposableThreadGroupAspect(String associateId, ThreadGroup threadGroup, T entity) {
        super(associateId, threadGroup);
        this.hasRelease = Boolean.FALSE;
        this.entity = entity;
    }

    public DisposableThreadGroupAspect<T> release(){
        this.hasRelease = Boolean.TRUE;
        return this;
    }

    public boolean hasRelease(){
        return this.hasRelease;
    }

    public T getEntity(){
        return entity;
    }
}
