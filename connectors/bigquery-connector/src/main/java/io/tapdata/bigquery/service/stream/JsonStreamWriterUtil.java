package io.tapdata.bigquery.service.stream;

import com.google.cloud.bigquery.storage.v1.*;
import io.tapdata.entity.error.CoreException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class JsonStreamWriterUtil {
    public static JsonStreamWriterUtil create(String projectId,String dataSet){
        if (Objects.isNull(util)){
            synchronized (JsonStreamWriterUtil.class){
                util = new JsonStreamWriterUtil();
            }
        }
        return util.projectId(projectId).dataSet(dataSet);
    }
    private JsonStreamWriterUtil(){

    }
    private String projectId;
    public String projectId(){return this.projectId;}
    public JsonStreamWriterUtil projectId(String projectId){
        this.projectId = projectId;
        return this;
    }
    private String dataSet;
    public String dataSet(){return this.dataSet;}
    public JsonStreamWriterUtil dataSet(String dataSet){
        this.dataSet = dataSet;
        return this;
    }
    private static JsonStreamWriterUtil util;
    private static final Map<String ,JsonStreamWriter> jsonStreamWriterMap = new HashMap<>();
    public static JsonStreamWriter getWriteStreamMap(String tableName){
        return Optional.ofNullable(jsonStreamWriterMap.get(tableName))
                .orElse(new JsonStreamWriterUtil().createWriteStream(tableName));
    }

    public JsonStreamWriter createWriteStream(String table) {
        if (Objects.isNull(projectId)) throw new CoreException("Project id must not be null or not be empty.");
        if (Objects.isNull(dataSet)) throw new CoreException("DataSet must not be null or not be empty.");
        try {
            BigQueryWriteClient bqClient = BigQueryWriteClient.create();
            WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
            TableName tableName = TableName.of(this.projectId(),this.dataSet(), table);
            CreateWriteStreamRequest createwriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(tableName.toString()).setWriteStream(stream)
                    .build();
            WriteStream writeStream = bqClient.createWriteStream(createwriteStreamRequest);
            JsonStreamWriter jsonStreamWriter = JsonStreamWriter
                    .newBuilder(writeStream.getName(), writeStream.getTableSchema()).build();

            jsonStreamWriterMap.put(table,jsonStreamWriter);
            return jsonStreamWriter;
        }catch (Exception e){
            return null;
        }
    }
    public JsonStreamWriter createWriteStream(String projectId,String dataSet,String table){
        return create(projectId, dataSet).createWriteStream(table);
    }
}
