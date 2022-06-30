package io.tapdata.pdk.apis.entity;

import java.util.ArrayList;
import java.util.List;

public class Projection {
    private List<String> includeFields;
    private List<String> excludeFields;
    public static Projection create() {
        return new Projection();
    }

    public Projection include(String includeField) {
        if(includeFields == null)
            includeFields = new ArrayList<>();

        if((excludeFields == null || !excludeFields.contains(includeField)) && !includeFields.contains(includeField))
            includeFields.add(includeField);
        return this;
    }

    public Projection exclude(String excludeField) {
        if(excludeFields == null)
            excludeFields = new ArrayList<>();

        if((includeFields == null || !includeFields.contains(excludeField)) && !excludeFields.contains(excludeField))
            excludeFields.add(excludeField);
        return this;
    }

    public List<String> getIncludeFields() {
        return includeFields;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

}
