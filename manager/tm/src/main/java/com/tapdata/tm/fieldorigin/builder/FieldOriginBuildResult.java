package com.tapdata.tm.fieldorigin.builder;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class FieldOriginBuildResult {
    private List<FieldOriginRecord> records = new ArrayList<>();
    private FieldOriginStats stats = new FieldOriginStats();
    private String configHash;
}
