package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;
import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

@NodeType("migrate_add_date_field_processor")
@Getter
@Setter
@Slf4j
public class MigrateAddDateFieldProcessorNode extends MigrateProcessorNode{
    @EqField
    private String dateFieldName;

    public MigrateAddDateFieldProcessorNode() {
        super(NodeEnum.migrate_field_rename_processor.name(), NodeCatalog.processor);
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }
        List<Schema> outputSchemas = inputSchemas.get(0);
        if (StringUtils.isBlank(dateFieldName)) {
            return outputSchemas;
        }
        outputSchemas.forEach(schema -> {
            List<String> fieldNames = schema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
            boolean fieldExistFlag = fieldNames.stream().anyMatch((fieldName) -> dateFieldName.equals(fieldName));
            if (!fieldExistFlag) {
                FieldProcessorNode.Operation fieldOperation = new FieldProcessorNode.Operation();
                fieldOperation.setType("Date");
                fieldOperation.setField(dateFieldName);
                fieldOperation.setOp("CREATE");
                fieldOperation.setTableName(schema.getName());
                Field field = createField(this.getId(), schema.getOriginalName(), fieldOperation);
                field.setSource("job_analyze");
                field.setPrimaryKey(false);
                field.setIsNullable(false);
                field.setTapType(FieldModTypeProcessorNode.calTapType(field.getDataType()));
                schema.getFields().add(field);
            }
        });
        return outputSchemas;
    }


}
