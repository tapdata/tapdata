package com.tapdata.tm.metadatainstance.bean;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import lombok.Data;

import java.util.List;

@Data
public class MultiPleTransformReq {
    private List<FieldChangeRule> rules;
    private String qualifiedName;
    private String nodeId;
    private String databaseType;
    private List<Field> fields;
}
