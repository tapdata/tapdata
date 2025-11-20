package com.tapdata.tm.taskinspect.vo;

import com.tapdata.tm.taskinspect.TaskInspectUtils;
import com.tapdata.tm.taskinspect.cons.DiffTypeEnum;
import com.tapdata.tm.utils.MD5Utils;
import io.tapdata.entity.schema.value.DateTime;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 差异上报结构
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/14 18:03 Create
 */
@Getter
@Setter
public class ResultsReportVo implements Serializable {
    private String rowId;
    private LinkedHashMap<String, Object> keys;
    private String serializedKeys;
    private String sourceTable;
    private List<String> sourceFields;
    private LinkedHashMap<String, Object> source;
    private String targetTable;
    private List<String> targetFields;
    private LinkedHashMap<String, Object> target;
    private DiffTypeEnum diffType;
    private List<String> diffFields;
    private List<ResultOperationsVo> operations;

    public String getSerializedKeys() {
        if (null == serializedKeys && null != keys) {
            serializedKeys = TaskInspectUtils.keysSerialization(keys);
        }
        return serializedKeys;
    }

    public ResultsReportVo table(String rowId) {
        this.rowId = rowId;
        return this;
    }

    public ResultsReportVo keys(LinkedHashMap<String, Object> keys) {
        this.keys = keys;
        return this;
    }

    public ResultsReportVo sourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public ResultsReportVo sourceFields(List<String> sourceFields) {
        this.sourceFields = sourceFields;
        return this;
    }

    public ResultsReportVo source(LinkedHashMap<String, Object> source) {
        this.source = formatValue(source);
        return this;
    }

    public ResultsReportVo targetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public ResultsReportVo targetFields(List<String> targetFields) {
        this.targetFields = targetFields;
        return this;
    }

    public ResultsReportVo target(LinkedHashMap<String, Object> target) {
        this.target = formatValue(target);
        return this;
    }

    public ResultsReportVo diffType(DiffTypeEnum diffType) {
        this.diffType = diffType;
        return this;
    }

    public ResultsReportVo diffFields(List<String> diffFields) {
        this.diffFields = diffFields;
        return this;
    }

    public ResultsReportVo operations(List<ResultOperationsVo> operations) {
        this.operations = operations;
        return this;
    }

    public static ResultsReportVo create(DiffTypeEnum diffType, String rowId) {
        return new ResultsReportVo().diffType(diffType).table(rowId);
    }

    protected LinkedHashMap<String, Object> formatValue(LinkedHashMap<String, Object> valueMap) {
        for (Map.Entry<String, Object> en : valueMap.entrySet()) {
            if (en.getValue() instanceof DateTime) {
                en.setValue(((DateTime) en.getValue()).toInstant().toString());
            } else if (en.getValue() instanceof byte[]) {
                en.setValue(MD5Utils.toLowerHex((byte[]) en.getValue()));
            }
        }
        return valueMap;
    }

}
