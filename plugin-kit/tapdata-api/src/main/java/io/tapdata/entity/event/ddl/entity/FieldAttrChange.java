package io.tapdata.entity.event.ddl.entity;

public class FieldAttrChange<T> {
    private String name;
    public FieldAttrChange<T> name(String name) {
        this.name = name;
        return this;
    }
    private T before;
    public FieldAttrChange<T> before(T before) {
        this.before = before;
        return this;
    }
    private T after;
    public FieldAttrChange<T> after(T after) {
        this.after = after;
        return this;
    }

    public static <T> FieldAttrChange<T> create(String name) {
        return new FieldAttrChange<>(name);
    }
    public static <T> FieldAttrChange<T> create(String name, T after) {
        return new FieldAttrChange<>(name, after);
    }

    public static <T> FieldAttrChange<T> create(String name, T before, T after) {
        return new FieldAttrChange<>(name, before, after);
    }

    public FieldAttrChange() {
    }
    public FieldAttrChange(String name) {
        this(name, null);
    }
    public FieldAttrChange(String name, T after) {
        this(name, null, after);
    }
    public FieldAttrChange(String name, T before, T after) {
        this.name = name;
        this.before = before;
        this.after = after;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
