package io.tapdata.bigquery.service.stream;

import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import io.tapdata.entity.error.CoreException;

import java.io.ByteArrayInputStream;
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
    public JsonStreamWriter getWriteStreamMap(String tableName){
        return Optional.ofNullable(jsonStreamWriterMap.get(tableName))
                .orElse(util.createWriteStream(tableName));
    }

    String credentialsJson = "";
    public JsonStreamWriter createWriteStream(String table) {
        if (Objects.isNull(util.projectId)) throw new CoreException("Project id must not be null or not be empty.");
        if (Objects.isNull(util.dataSet)) throw new CoreException("DataSet must not be null or not be empty.");
        try {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes("utf8")));
            BigQueryWriteClient bqClient = BigQueryWriteClient.create();
            WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
            TableName tableName = TableName.of(util.projectId(),util.dataSet(), table);
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
