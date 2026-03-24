package com.tapdata.tm.group.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DagChangeDetail {
    private List<FieldChange> nodeAdditions = new ArrayList<>();
    private List<FieldChange> nodeRemovals = new ArrayList<>();
    private List<FieldChange> nodeConfigChanges = new ArrayList<>();
    private List<FieldChange> edgeAdditions = new ArrayList<>();
    private List<FieldChange> edgeRemovals = new ArrayList<>();

    public boolean hasChanges() {
        return !nodeAdditions.isEmpty()
                || !nodeRemovals.isEmpty()
                || !nodeConfigChanges.isEmpty()
                || !edgeAdditions.isEmpty()
                || !edgeRemovals.isEmpty();
    }

    public List<FieldChange> toFlatList() {
        List<FieldChange> all = new ArrayList<>();
        all.addAll(nodeAdditions);
        all.addAll(nodeRemovals);
        all.addAll(nodeConfigChanges);
        all.addAll(edgeAdditions);
        all.addAll(edgeRemovals);
        return all;
    }
}
