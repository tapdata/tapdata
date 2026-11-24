package com.tapdata.tm.commons.dag.process.script;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 脚本类节点（JS/Python/自定义）在推演服务无法试运行时，基于节点上次输出模型叠加字段级 DDL 变化，
 * 避免以输入模型代替脚本输出而丢失脚本新增、删除或转换的字段。
 */
public final class ScriptDdlSchemaMerger {

    private ScriptDdlSchemaMerger() {
    }

    public static boolean isFieldDdl(TapDDLEvent event) {
        return event instanceof TapNewFieldEvent || event instanceof TapAlterFieldAttributesEvent
                || event instanceof TapAlterFieldNameEvent || event instanceof TapDropFieldEvent;
    }

    /**
     * DDL 表名为源表名：优先按 ancestorsName（经表编辑后仍保留源表名）对应，否则按 originalName
     */
    public static boolean isDdlTable(Schema input, TapDDLEvent ddlEvent) {
        if (null == input || null == ddlEvent || null == ddlEvent.getTableId()) {
            return false;
        }
        String tableId = ddlEvent.getTableId();
        if (null != input.getAncestorsName()) {
            return tableId.equals(input.getAncestorsName());
        }
        return tableId.equals(input.getOriginalName());
    }

    /**
     * 以上次输出模型为基础叠加字段级 DDL；DDL 字段无法在输入模型中对应时返回 null，由调用方退回输入模型。
     * ddlEvent 为 null 表示本表不受此次 DDL 影响，直接沿用上次输出字段。
     */
    public static Schema applyDdlOnPrevious(Schema input, Schema previous, TapDDLEvent ddlEvent) {
        Schema previousCopy = SchemaUtils.cloneSchema(previous);
        List<Field> fields = new ArrayList<>();
        for (Field field : Optional.ofNullable(previousCopy.getFields()).orElse(new ArrayList<>())) {
            if (null != field && !field.isDeleted()) {
                fields.add(field);
            }
        }
        List<TableIndex> previousIndices = Optional.ofNullable(previousCopy.getIndices()).orElse(new ArrayList<>());
        if (ddlEvent instanceof TapNewFieldEvent) {
            List<TapField> newFields = ((TapNewFieldEvent) ddlEvent).getNewFields();
            if (CollectionUtils.isEmpty(newFields)) {
                return null;
            }
            for (TapField newField : newFields) {
                Field inputField = newField == null ? null : findField(input.getFields(), newField.getName());
                if (null == inputField) {
                    return null;
                }
                replaceOrAppend(fields, newField.getName(), inputField);
            }
        } else if (ddlEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent alterEvent = (TapAlterFieldAttributesEvent) ddlEvent;
            Field inputField = findField(input.getFields(), alterEvent.getFieldName());
            if (null == inputField) {
                return null;
            }
            // 脚本已删除的字段不因属性变更而恢复；仍存在的字段只叠加本次 DDL 实际改变的属性，保留脚本的改名与类型转换
            int index = indexOfField(fields, alterEvent.getFieldName());
            if (index >= 0) {
                applyAttributeChange(fields.get(index), inputField, alterEvent);
            }
        } else if (ddlEvent instanceof TapAlterFieldNameEvent) {
            ValueChange<String> nameChange = ((TapAlterFieldNameEvent) ddlEvent).getNameChange();
            if (null == nameChange || null == nameChange.getAfter()) {
                return null;
            }
            Field inputField = findField(input.getFields(), nameChange.getAfter());
            if (null == inputField) {
                return null;
            }
            int index = indexOfField(fields, nameChange.getBefore());
            if (index >= 0) {
                fields.set(index, inputField);
            } else {
                replaceOrAppend(fields, nameChange.getAfter(), inputField);
            }
            renameIndexColumn(previousIndices, nameChange.getBefore(), inputField.getFieldName());
        } else if (ddlEvent instanceof TapDropFieldEvent) {
            removeField(fields, ((TapDropFieldEvent) ddlEvent).getFieldName());
        }

        Schema merged = SchemaUtils.cloneSchema(input);
        merged.setFields(fields);
        Set<String> fieldNames = fields.stream().map(Field::getFieldName).filter(Objects::nonNull).collect(Collectors.toSet());
        List<TableIndex> indices = new ArrayList<>();
        for (TableIndex index : previousIndices) {
            if (null == index || CollectionUtils.isEmpty(index.getColumns())) {
                continue;
            }
            boolean allColumnsExist = index.getColumns().stream()
                    .allMatch(c -> null != c && fieldNames.contains(c.getColumnName()));
            if (allColumnsExist) {
                indices.add(index);
            }
        }
        merged.setIndices(indices);
        return merged;
    }

    /**
     * 属性变更只覆盖本次 DDL 改变的属性。类型变更时，若上次输出类型与变更前的源类型不同，说明脚本做过类型转换，保留脚本类型。
     */
    static void applyAttributeChange(Field output, Field input, TapAlterFieldAttributesEvent event) {
        ValueChange<String> dataTypeChange = event.getDataTypeChange();
        if (null != dataTypeChange && !convertedByScript(output, dataTypeChange)) {
            output.setDataType(input.getDataType());
            output.setDataType1(input.getDataType1());
            output.setTapType(input.getTapType());
            output.setSimpleTypeName(input.getSimpleTypeName());
            output.setPureDataType(input.getPureDataType());
            output.setLength(input.getLength());
            output.setColumnSize(input.getColumnSize());
            output.setPrecision(input.getPrecision());
            output.setScale(input.getScale());
            output.setOriPrecision(input.getOriPrecision());
            output.setOriScale(input.getOriScale());
            output.setDataCode(input.getDataCode());
            output.setJavaType(input.getJavaType());
            output.setJavaType1(input.getJavaType1());
            output.setOriginalDataType(input.getOriginalDataType());
            output.setFixed(input.getFixed());
            output.setDataTypeSupport(input.getDataTypeSupport());
        }
        if (null != event.getNullableChange()) {
            output.setIsNullable(input.getIsNullable());
        }
        if (null != event.getDefaultChange()) {
            output.setDefaultValue(input.getDefaultValue());
            output.setDefaultFunction(input.getDefaultFunction());
            output.setOriginalDefaultValue(input.getOriginalDefaultValue());
        }
        if (null != event.getCommentChange()) {
            output.setComment(input.getComment());
        }
        if (null != event.getPrimaryChange()) {
            output.setPrimaryKey(input.getPrimaryKey());
            output.setPrimaryKeyPosition(input.getPrimaryKeyPosition());
        }
    }

    static boolean convertedByScript(Field output, ValueChange<String> dataTypeChange) {
        String before = dataTypeChange.getBefore();
        return null != before && null != output.getDataType() && !before.equalsIgnoreCase(output.getDataType());
    }

    static void renameIndexColumn(List<TableIndex> indices, String before, String after) {
        if (null == before || null == after) {
            return;
        }
        for (TableIndex index : indices) {
            if (null == index || null == index.getColumns()) {
                continue;
            }
            for (TableIndexColumn column : index.getColumns()) {
                if (null != column && before.equals(column.getColumnName())) {
                    column.setColumnName(after);
                }
            }
        }
    }

    /**
     * DDL 字段名为源字段名：优先按 originalFieldName 对应（兼容上游或脚本对字段改名），否则按字段名。
     * fields 必须非 null，可能为 null 的模型字段列表经 {@link #findField} 处理
     */
    static int indexOfField(List<Field> fields, String name) {
        if (null == name) {
            return -1;
        }
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (null != field && name.equals(field.getOriginalFieldName())) {
                return i;
            }
        }
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (null != field && name.equals(field.getFieldName())) {
                return i;
            }
        }
        return -1;
    }

    static Field findField(List<Field> fields, String name) {
        if (null == fields) {
            return null;
        }
        int index = indexOfField(fields, name);
        return index < 0 ? null : fields.get(index);
    }

    /**
     * 与 {@link #indexOfField} 使用同一对应规则，避免脚本改名字段与源字段同时出现
     */
    static void replaceOrAppend(List<Field> fields, String name, Field field) {
        int index = indexOfField(fields, name);
        if (index >= 0) {
            fields.set(index, field);
        } else {
            fields.add(field);
        }
    }

    static void removeField(List<Field> fields, String name) {
        int index = indexOfField(fields, name);
        if (index >= 0) {
            fields.remove(index);
        }
    }
}
