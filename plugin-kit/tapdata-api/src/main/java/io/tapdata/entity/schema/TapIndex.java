package io.tapdata.entity.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TapIndex implements Serializable {
    /**
     * Index name
     */
    private String name;
    public TapIndex name(String name) {
        this.name = name;
        return this;
    }
    /**
     * Index fields
     */
    private List<TapIndexField> indexFields;
    public TapIndex indexField(TapIndexField indexField) {
        if(indexFields == null)
            indexFields = new ArrayList<>();
        indexFields.add(indexField);
        return this;
    }

    private Boolean unique;
    public TapIndex unique(boolean unique) {
        this.unique = unique;
        return this;
    }

    private Boolean primary;
    public TapIndex primary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public List<TapIndexField> getIndexFields() {
        return indexFields;
    }

    public void setIndexFields(List<TapIndexField> indexFields) {
        this.indexFields = indexFields;
    }

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

    public boolean isUnique() {
        return unique != null && unique;
    }

    public boolean isPrimary() {
        return primary != null && primary;
    }

    public Boolean getPrimary() {
        return primary;
    }

    public void setPrimary(Boolean primary) {
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
