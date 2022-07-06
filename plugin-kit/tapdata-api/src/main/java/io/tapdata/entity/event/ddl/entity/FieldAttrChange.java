package io.tapdata.entity.event.ddl.entity;

public class FieldAttrChange<T> extends ValueChange<T> {
    private String name;
    public FieldAttrChange<T> name(String name) {
        this.name = name;
        return this;
    }

    public void clone(FieldAttrChange<T> fieldAttrChange) {
        if(fieldAttrChange != null) {
            fieldAttrChange.name = name;
            super.clone(fieldAttrChange);
        }
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
        super(before, after);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
