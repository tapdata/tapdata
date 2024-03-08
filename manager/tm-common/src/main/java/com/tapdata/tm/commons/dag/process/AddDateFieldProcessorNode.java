package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

@NodeType("add_date_field_processor")
@Getter
@Setter
@Slf4j
public class AddDateFieldProcessorNode extends ProcessorNode{
    @EqField
    private String dateFieldName;

    public AddDateFieldProcessorNode() {
        super("add_date_field_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = super.mergeSchema(inputSchemas, schema, options);
        if (StringUtils.isNotBlank(dateFieldName)) {
            FieldProcessorNode.Operation fieldOperation = new FieldProcessorNode.Operation();
            fieldOperation.setType("Date");
            fieldOperation.setField(dateFieldName);
            fieldOperation.setOp("CREATE");
            fieldOperation.setTableName(outputSchema.getName());
            Field field = createField(this.getId(), outputSchema.getOriginalName(), fieldOperation);
            field.setSource("job_analyze");
            field.setTapType(FieldModTypeProcessorNode.calTapType(field.getDataType()));
            outputSchema.getFields().add(field);
        }
        return outputSchema;
    }
}
