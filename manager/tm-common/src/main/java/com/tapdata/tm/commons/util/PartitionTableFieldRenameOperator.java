package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapPartitionField;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PartitionTableFieldRenameOperator {
    Map<String, String> renameMap;

    public PartitionTableFieldRenameOperator() {
    }

    public void startAt() {
        renameMap = new HashMap<>();
    }

    public PartitionTableFieldRenameOperator rename(String originName, String newName) {
        if (Objects.isNull(renameMap)) startAt();
        renameMap.put(originName, newName);
        return this;
    }

    protected void updatePartitionField(Schema outputSchema) {
        if (Objects.isNull(renameMap) || renameMap.isEmpty()) return;
        TapPartition partitionInfo = outputSchema.getPartitionInfo();
        if (Objects.isNull(partitionInfo)) return;
        List<TapPartitionField> partitionFields = partitionInfo.getPartitionFields();
        if (CollectionUtils.isEmpty(partitionFields)) return;
        partitionFields.stream()
                .filter(Objects::nonNull)
                .filter(f -> renameMap.containsKey(f.getName()) && Objects.nonNull(renameMap.get(f.getName())))
                .forEach(f -> f.setName(renameMap.get(f.getName())));
    }

    public void endOf(Schema outputSchema) {
        updatePartitionField(outputSchema);
        renameMap = null;
    }
}
