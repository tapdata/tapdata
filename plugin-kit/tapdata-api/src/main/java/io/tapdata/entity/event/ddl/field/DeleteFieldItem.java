package io.tapdata.entity.event.ddl.field;

public class DeleteFieldItem extends TapFieldItem {
    public static final int TYPE = 102;
    private String name;

    public DeleteFieldItem() {
        super(TYPE);
    }

    public DeleteFieldItem name(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
