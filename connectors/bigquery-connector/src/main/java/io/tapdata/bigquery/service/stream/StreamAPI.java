package io.tapdata.bigquery.service.stream;

import com.google.cloud.bigquery.storage.v1.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import org.json.JSONArray;
import org.json.JSONObject;

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
        firstTableJsonObj.put("ID", 147584765);
        firstTableJsonObj.put("TYPE_BOOLEAN", "true");
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
        StreamAPI api = new StreamAPI();
        JSONArray jsonArr = new JSONArray();
        ArrayList<TapRecordEvent> objects = new ArrayList<>();

        api.updateRequestMetadataOperations(jsonArr,objects);
        try {
            api.insertIntoBigQuery("All_Type_test",jsonArr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
