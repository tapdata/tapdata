package io.tapdata.entity.event.ddl.field;

public class UpdateFieldItem extends TapFieldItem {
    public static final int TYPE = 100;
    private String name;
    public UpdateFieldItem name(String name) {
        this.name = name;
        return this;
    }
    private String tapFieldKey;
    public UpdateFieldItem tapFieldKey(String field) {
        this.tapFieldKey = field;
        return this;
    }
    private String oldValue;
    public UpdateFieldItem oldValue(String oldValue) {
        this.oldValue = oldValue;
        return this;
    }
    private String newValue;
    public UpdateFieldItem newValue(String newValue) {
        this.newValue = newValue;
        return this;
    }
    private Boolean deleted;
    public UpdateFieldItem deleted(Boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public UpdateFieldItem() {
        super(TYPE);
    }

    public String getTapFieldKey() {
        return tapFieldKey;
    }

    public void setTapFieldKey(String tapFieldKey) {
        this.tapFieldKey = tapFieldKey;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
