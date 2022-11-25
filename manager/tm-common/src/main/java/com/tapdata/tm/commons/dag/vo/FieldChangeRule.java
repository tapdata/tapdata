package com.tapdata.tm.commons.dag.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * 字段变更规则
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/22 17:09 Create
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldChangeRule implements Serializable {

    public enum Scope {
        Node, Table, Field,
        ;
    }

    public enum Type {
        DataType,
        ;
    }

    private String id;
    private Scope scope;
    private String[] namespace;//[nodeId,qualifiedName,fieldName]
    private Type type;
    private String accept;
    private Map<String, String> result;


    public String getNodeId() {
        return (null == namespace || namespace.length < 1) ? null : namespace[0];
    }

    public String getQualifiedName() {
        return (null == namespace || namespace.length < 2) ? null : namespace[1];
    }

    public String getFieldName() {
        return (null == namespace || namespace.length < 3) ? null : namespace[2];
    }
}
