package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

@PatchAnnotation(appType = AppType.DAAS, version = "3.5-12")
public class v3_5_12_DDLFilter_Fix extends AbsPatch {
    public v3_5_12_DDLFilter_Fix(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        TaskRepository taskRepository = SpringContextHelper.getBean(TaskRepository.class);
        MongoTemplate mongoTemplate = taskRepository.getMongoOperations();
        MongoCollection<Document> collection = mongoTemplate.getCollection("Task");
        Document filterTrue = new Document("dag.nodes.enableDDL",true);
        Document updateSetTrue = new Document();
        updateSetTrue.append("dag.nodes.$.ddlConfiguration","SYNCHRONIZATION");
        Document remove = new Document();
        remove.append("dag.nodes.$.enableDDL","");
        Document updateTrue = new Document("$set", updateSetTrue);
        updateTrue.append("$unset",remove);
        collection.updateMany(filterTrue,updateTrue);
        Document filterFalse = new Document("dag.nodes.enableDDL",false);
        Document updateSetFalse = new Document();
        updateSetFalse.append("dag.nodes.$.ddlConfiguration","FILTER");
        Document updateFalse = new Document("$set", updateSetFalse);
        updateFalse.append("$unset",remove);
        collection.updateMany(filterFalse,updateFalse);
    }
}
