package io.tapdata.aspect;

public abstract class AbsDataNodeErrorAspect<T extends AbsDataNodeErrorAspect<?>> extends DataNodeAspect<T> {

    private Throwable error;

    public Throwable getError() {
        return error;
    }

    public T error(Throwable error) {
        this.error = error;
        return (T) this;
    }
}
