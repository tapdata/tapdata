package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codecs.TDDUser;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TapUtilsImplTest {
    @Test
    public void test() {
        TapUtils tapUtils = InstanceFactory.instance(TapUtils.class);

        TDDUser user = new TDDUser();
        user.setUserId("abc");
        user.setName("aaa");

        List<TDDUser> users = new ArrayList<>();
        users.add(user);

        Map<String, Object> userMap = new HashMap<>();
        userMap.put("aaa", user);
        userMap.put("bbb", user);
        Map<String, Object> newUserMap = tapUtils.cloneMap(userMap);
        assertEquals(userMap.size(), newUserMap.size());
        assertEquals(((TDDUser)userMap.get("aaa")).getName(), ((TDDUser)newUserMap.get("aaa")).getName());
        assertEquals(((TDDUser)userMap.get("bbb")).getName(), ((TDDUser)newUserMap.get("bbb")).getName());

        Document document = new Document();
        BsonTimestamp bsonTimestamp = new BsonTimestamp(new Date().getTime());
        ObjectId objectId = ObjectId.get();
        document.put("timestamp", bsonTimestamp);
        document.put("oid", objectId);
        Document embeddedDocument = new Document();
        embeddedDocument.put("aaa", 123);
        document.put("doc", embeddedDocument);
        Document newDocument = (Document) tapUtils.cloneMap(document);
        assertEquals(((BsonTimestamp)newDocument.get("timestamp")).getValue(), bsonTimestamp.getValue());
        assertEquals(newDocument.get("oid").toString(), objectId.toString());
        assertEquals(((Document)newDocument.get("doc")).get("aaa"), 123);

        List<Document> documents = new ArrayList<>();
        documents.add(document);

        Map<TDDUser, Document> documentMap = new HashMap<>();
        documentMap.put(user, document);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("aaa", "BBB");
        JSONObject newJsonObject = (JSONObject) tapUtils.cloneMap(jsonObject);
        assertEquals(jsonObject.get("aaa"), newJsonObject.get("aaa"));

        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("aaa", "BBB");
        jsonObject1.put("documentMap", documentMap);
        jsonObject1.put("documents", documents);
        jsonObject1.put("userMap", userMap);
        jsonObject1.put("users", users);
        jsonObject1.put("document", document);
        JSONObject newJsonObject1 = (JSONObject) tapUtils.cloneMap(jsonObject1);
        assertEquals(jsonObject1.get("aaa"), newJsonObject1.get("aaa"));
        TDDUser userKey1 = ((Map<TDDUser, Document>)newJsonObject1.get("documentMap")).keySet().stream().findFirst().get();
        assertEquals(user.getName(), userKey1.getName());
        TDDUser userKey = documentMap.keySet().stream().findFirst().get();
        Document doc3 = documentMap.get(userKey);
        assertEquals(((BsonTimestamp)doc3.get("timestamp")).getValue(), bsonTimestamp.getValue());
        assertEquals(doc3.get("oid").toString(), objectId.toString());
        assertEquals(((Document)doc3.get("doc")).get("aaa"), 123);

    }

}
