package io.tapdata.bigquery.service.stream;

import com.google.cloud.bigquery.storage.v1.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StreamAPI {
    public void updateRequestMetadataOperations(JSONArray firstObjArr, List<TapRecordEvent> objectList) {
        if (Objects.isNull(objectList)) return;
        for (TapRecordEvent event : objectList) {
            JSONObject firstTableJsonObj = new JSONObject();
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
//            firstTableJsonObj.put("", "");
            firstObjArr.put(firstTableJsonObj);
        }
        JSONObject firstTableJsonObj = new JSONObject();
        firstTableJsonObj.put("_id", 147584765);
        //firstTableJsonObj.put("T_BIGINT", "true");
        firstObjArr.put(firstTableJsonObj);
    }

    public void insertIntoBigQuery(String tableName,JSONArray jsonArr) throws Exception {
        if (jsonArr.isEmpty()) {
            return;
        }
        JsonStreamWriter jsonStreamwriter = JsonStreamWriterUtil
                .create("vibrant-castle-366614","SchemaoOfJoinSet")
                .getWriteStreamMap(tableName);
        if (jsonStreamwriter != null) {
            jsonStreamwriter.append(jsonArr);
        }
    }

    public static void main(String[] args) {

    }


    public WriteStream writeStream(String projectId, String dataSet, String table){
        try (BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create()) {
            TableName parent = TableName.of(projectId, dataSet, table);
            WriteStream writeStream = WriteStream.newBuilder().build();
            return bigQueryWriteClient.createWriteStream(parent, writeStream);
        } catch (IOException e) {
            //e.printStackTrace();
        }
        return null;
    }
}
