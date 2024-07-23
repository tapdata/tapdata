package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

@PatchAnnotation(appType = AppType.DAAS, version = "3.10-1")
public class V3_10_1_Task_Last_Updated_Index extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V3_10_1_Task_Last_Updated_Index.class);

    public V3_10_1_Task_Last_Updated_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "Task";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Index index = new Index().named("index_task_last_updated").on("last_updated", Sort.Direction.DESC).background();
        mongoTemplate.indexOps(collectionName).ensureIndex(index);
    }
}
