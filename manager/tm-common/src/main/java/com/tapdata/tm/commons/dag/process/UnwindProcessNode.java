package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

/**
 * @author GavinXiao
 * @description UnwindProcessNode create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
* */
@NodeType("unwind_processor")
@Slf4j
public class UnwindProcessNode extends ProcessorNode {
    public static final String SUF_PATH_KEY = "$";

    private String nodeName;

    /** Field path to an array field.
     * To specify a field path,
     * prefix the field name and enclose in quotes. */
    private String path;

    /** Optional.
     * The name of a new field to hold the array index of the element.
     * The name cannot start with a dollar sign $. */
    private String includeArrayIndex;

    /** Optional.
     If true, if the path is null, missing, or an empty array,
     $unwind
     outputs the document.
     If false, if path is null, missing, or an empty array,
     $unwind
     does not output a document.
     The default value is false. */
    private boolean preserveNullAndEmptyArrays;

    public UnwindProcessNode() {
        super("unwind_processor");
    }

    protected Schema superMergeSchema(List<Schema> inputSchemas, Schema schema) {
        return super.mergeSchema(inputSchemas, schema, null);
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = superMergeSchema(inputSchemas, schema);
        List<Field> fields = outputSchema.getFields();
        Map<String, Field> originFieldMap = fields.stream().collect(Collectors.toMap(Field::getFieldName, f -> f));
        if (originFieldMap.containsKey(path)) {
            fields.remove(originFieldMap.get(path));
            FieldProcessorNode.Operation fieldOperation = new FieldProcessorNode.Operation();
            fieldOperation.setType("Map");
            fieldOperation.setField(path);
            fieldOperation.setOp("CREATE");
            fieldOperation.setTableName(outputSchema.getName());
            fieldOperation.setJava_type("Map");
            Field field = createField(this.getId(), outputSchema.getOriginalName(), fieldOperation);
            field.setSource("job_analyze");
            field.setTapType(FieldModTypeProcessorNode.calTapType("Map"));
            outputSchema.getFields().add(field);
            if (null != includeArrayIndex && !"".equals(includeArrayIndex.trim())) {
                FieldProcessorNode.Operation operation = new FieldProcessorNode.Operation();
                operation.setType("Long");
                operation.setField(includeArrayIndex);
                operation.setOp("CREATE");
                operation.setTableName(outputSchema.getName());
                operation.setJava_type("Long");
                Field fieldIndex = createField(this.getId(), outputSchema.getOriginalName(), operation);
                fieldIndex.setSource("job_analyze");
                fieldIndex.setTapType(FieldModTypeProcessorNode.calTapType("Long"));
                outputSchema.getFields().add(fieldIndex);
            }
        }
        return outputSchema;
    }

    protected TapTable getTapTable(Node target, TaskDto taskDtoCopy) {
        return service.loadTapTable(getId(), target.getId(), taskDtoCopy);
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getPath() {
        if (null != path && path.startsWith(SUF_PATH_KEY) && !SUF_PATH_KEY.equals(path.trim())) return path.substring(1);
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getIncludeArrayIndex() {
        return includeArrayIndex;
    }

    public void setIncludeArrayIndex(String includeArrayIndex) {
        this.includeArrayIndex = includeArrayIndex;
    }

    public boolean isPreserveNullAndEmptyArrays() {
        return preserveNullAndEmptyArrays;
    }

    public void setPreserveNullAndEmptyArrays(boolean preserveNullAndEmptyArrays) {
        this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
    }
}
