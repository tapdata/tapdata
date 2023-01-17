package io.tapdata.bigquery.service.bigQuery;

import com.google.cloud.bigquery.*;

import java.util.*;

public class BigQueryResult {
    private final TableResult tableResult;

    public BigQueryResult(TableResult result) {
        this.tableResult = result;
    }

    public static BigQueryResult create(TableResult result) {
        return new BigQueryResult(result);
    }

    public List<Map<String, Object>> result() {
        if (Objects.isNull(this.tableResult)) return new ArrayList<>();
        Schema schema = this.tableResult.getSchema();
        if (Objects.isNull(schema)) return new ArrayList<>();
        FieldList fields = schema.getFields();
        if (Objects.isNull(fields) || fields.isEmpty()) return new ArrayList<>();
        Iterator<FieldValueList> iterator = this.tableResult.getValues().iterator();
        List<Map<String, Object>> result = new ArrayList<>();
        while (iterator.hasNext()) {
            FieldValueList next = iterator.next();
            Map<String, Object> line = new HashMap<>();
            for (int index = 0; index < fields.size(); index++) {
                Field field = fields.get(index);
                FieldValue fieldValue = next.get(index);
                if (Objects.nonNull(field)) {
                    line.put(field.getName(), Objects.isNull(fieldValue) ? null : fieldValue.getValue());
                }
            }
            result.add(line);
        }
        return result;
    }

    public long getTotalRows() {
        return this.tableResult.getTotalRows();
    }

    public Iterable<FieldValueList> getValues() {
        return this.tableResult.getValues();
    }
}
