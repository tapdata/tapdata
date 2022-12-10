package io.tapdata.entity.schema;

import java.io.Serializable;
import java.util.List;

public class TapIndexField implements Serializable {
    /**
     * Index name
     */
    private String name;
    public TapIndexField name(String name) {
        this.name = name;
        return this;
    }
    /**
     * Field is asc or not.
     */
    private Boolean fieldAsc;
    public TapIndexField fieldAsc(Boolean fieldAsc) {
        this.fieldAsc = fieldAsc;
        return this;
    }

    public Boolean getFieldAsc() {
        return fieldAsc;
    }

    public void setFieldAsc(Boolean fieldAsc) {
        this.fieldAsc = fieldAsc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "TapIndexField name " + name + " fieldAsc " + fieldAsc;
    }
}
