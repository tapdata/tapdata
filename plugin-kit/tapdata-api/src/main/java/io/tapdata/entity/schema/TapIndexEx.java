package io.tapdata.entity.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TapIndexEx extends TapIndex {
    private final Map<String, TapIndexField> indexMap = new LinkedHashMap<>();
    public TapIndexEx() {}
    public TapIndexEx(TapIndex tapIndex) {
        if(tapIndex != null) {
            List<TapIndexField> indexFields = tapIndex.getIndexFields();
            if(indexFields != null) {
                for(TapIndexField field : indexFields) {
                    indexMap.put(field.getName(), field);
                }
            }
            this.setIndexFields(indexFields);
            this.setCluster(tapIndex.getCluster());
            this.setName(tapIndex.getName());
            this.setPrimary(tapIndex.getPrimary());
            this.setUnique(tapIndex.getUnique());
        }
    }

    public Map<String, TapIndexField> getIndexMap() {
        return indexMap;
    }
}
