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
                replaceOrAppend(fields, inputField);
            }
        } else if (ddlEvent instanceof TapAlterFieldAttributesEvent) {
            Field inputField = findField(input.getFields(), ((TapAlterFieldAttributesEvent) ddlEvent).getFieldName());
            if (null == inputField) {
                return null;
            }
            // 脚本已删除的字段不因属性变更而恢复
            if (null != findField(fields, inputField.getFieldName())) {
                replaceOrAppend(fields, inputField);
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
            removeField(fields, nameChange.getBefore());
            replaceOrAppend(fields, inputField);
        } else if (ddlEvent instanceof TapDropFieldEvent) {
            removeField(fields, ((TapDropFieldEvent) ddlEvent).getFieldName());
        }

        Schema merged = SchemaUtils.cloneSchema(input);
        merged.setFields(fields);
        Set<String> fieldNames = fields.stream().map(Field::getFieldName).filter(Objects::nonNull).collect(Collectors.toSet());
        List<TableIndex> indices = new ArrayList<>();
        for (TableIndex index : Optional.ofNullable(previousCopy.getIndices()).orElse(new ArrayList<>())) {
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
     * DDL 字段名为源字段名：优先按 originalFieldName 对应（兼容上游字段改名），否则按字段名
     */
    static Field findField(List<Field> fields, String name) {
        if (null == fields || null == name) {
            return null;
        }
        for (Field field : fields) {
            if (null != field && name.equals(field.getOriginalFieldName())) {
                return field;
            }
        }
        for (Field field : fields) {
            if (null != field && name.equals(field.getFieldName())) {
                return field;
            }
        }
        return null;
    }

    static void replaceOrAppend(List<Field> fields, Field field) {
        for (int i = 0; i < fields.size(); i++) {
            if (Objects.equals(fields.get(i).getFieldName(), field.getFieldName())) {
                fields.set(i, field);
                return;
            }
        }
        fields.add(field);
    }

    static void removeField(List<Field> fields, String name) {
        Field field = findField(fields, name);
        if (null != field) {
            fields.remove(field);
        }
    }
}
