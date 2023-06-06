package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;


@PatchAnnotation(appType = AppType.DAAS, version = "3.1-0")
public class V3_1_0_External_Storage extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V3_1_0_External_Storage.class);

    public V3_1_0_External_Storage(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());

        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        long counted = mongoTemplate.getCollection("TapExternalStorage").countDocuments();
        if (counted <= 0) {
            ExternalStorageService externalStorageService = SpringContextHelper.getBean(ExternalStorageService.class);
            UpdateResult updateResult = externalStorageService.update(new Query(Criteria.where("name").is("Tapdata MongoDB External Storage")), new Update().unset("table"));
            logger.info("updateResult: {}", updateResult);
        }

    }
}
