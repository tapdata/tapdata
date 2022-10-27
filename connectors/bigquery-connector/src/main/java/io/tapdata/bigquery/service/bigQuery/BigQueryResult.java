package io.tapdata.bigquery.service.bigQuery;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;

import java.util.*;

public class BigQueryResult {
    TableResult tableResult;
    public BigQueryResult(TableResult result) {
        this.tableResult= result;
    }

    public static BigQueryResult create(TableResult result){
        return new BigQueryResult(result);
    }

    public List<Map<String,Object>> result(){
        Schema schema = this.tableResult.getSchema();
        if (null == schema) return new ArrayList<>();
        FieldList fields = schema.getFields();
        if (null == fields && fields.isEmpty()) return new ArrayList<>();
        Iterator<FieldValueList> iterator = this.tableResult.getValues().iterator();
        if (null == iterator ) return new ArrayList<>();
        List<Map<String,Object>> result = new ArrayList<>();
        while (iterator.hasNext()){
            FieldValueList next = iterator.next();
            Map<String,Object> line = new HashMap<>();
            for (int index=0;index<fields.size();index++) {
                line.put(fields.get(index).getName(),next.get(index));
            }
            result.add(line);
        }
        return result;
    }

    public long getTotalRows(){
        return this.tableResult.getTotalRows();
    }
    public Iterable<FieldValueList> getValues(){
        return this.tableResult.getValues();
    }
}
