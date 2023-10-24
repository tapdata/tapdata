package io.tapdata.entity.event.ddl.entity;

public class ValueChange<T> {
    private T before;
    public ValueChange<T> before(T before) {
        this.before = before;
        return this;
    }
    private T after;
    public ValueChange<T> after(T after) {
        this.after = after;
        return this;
    }

    public static <T> ValueChange<T> create() {
        return new ValueChange<>();
    }
    public static <T> ValueChange<T> create(T after) {
        return new ValueChange<>(after);
    }

    public static <T> ValueChange<T> create(T before, T after) {
        return new ValueChange<>(before, after);
    }

    public ValueChange() {
    }
    public ValueChange(T after) {
        this(null, after);
    }
    public ValueChange(T before, T after) {
        this.before = before;
        this.after = after;
    }

    public T getBefore() {
        return before;
    }

    public void setBefore(T before) {
        this.before = before;
    }

    public T getAfter() {
        return after;
    }

    public void setAfter(T after) {
        this.after = after;
    }

    public void clone(ValueChange<T> valueChange) {
        if(valueChange != null) {
            valueChange.before = before;
            valueChange.after = after;
        }
    }
}
