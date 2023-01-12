package com.tapdata.tm.ds.service.impl;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: Zed
 * @Date: 2022/2/26
 * @Description:
 */
@Component
public class RepairCreateTimeComponent {

    private static final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
    private static final SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    @Autowired
    private MongoTemplate mongoTemplate;



    public void repair() {
        try {
            Query query = new Query();
            query.fields().include("createTime");
            List<Document> documents = mongoTemplate.find(query, Document.class, "Connections");

            Map<Object, Date> updateBatch = new HashMap<>();
            int num = 0;
            for (Document document : documents) {
                Object createTime = document.get("createTime");
                if (createTime instanceof String) {
                    Date value = null;
                    try {
                        if (((String) createTime).endsWith("Z")) {
                            String d = (String) createTime;
                            d = d.replace("T", " ");
                            d = d.replace("Z", "");
                            value = format1.parse(d);
                        } else {
                            value = format.parse((String) createTime);
                        }
                    } catch (Exception e) {
                        //value = new Date();
                    }
                    if (value != null) {
                        num++;
                        updateBatch.put(document.get("_id"), value);
                    }
                    if (num >= 100) {
                        bulkUpdate(updateBatch);
                        num = 0;
                        updateBatch.clear();
                    }
                }
            }

            bulkUpdate(updateBatch);
        } catch (Exception e) {
        }
    }


    public void bulkUpdate(Map<Object, Date> updateBatch) {
        BulkOperations operations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, "Connections");
        for (Map.Entry<Object, Date> entry : updateBatch.entrySet()) {
            operations.updateOne(new Query(Criteria.where("_id").is(entry.getKey())), Update.update("createTime", entry.getValue()));
        }
        operations.execute();
    }

}
