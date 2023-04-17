package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;


/**
 *
 * @author <a href="mailto:meteor209@gmail.com">jiuye</a>
 * @version v1.0 2023/2/10 15:47 Create
 */
@PatchAnnotation(appType = AppType.DAAS, version = "2.16-1")
public class V2_16_1_API_APP extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_16_1_API_APP.class);

    public V2_16_1_API_APP(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Criteria criteria1 = Criteria.where("value").is("Default")
                .and("readOnly").is(true)
                .and("itemType").is("app");
        MetadataDefinitionEntity metadataDefinition = mongoTemplate.findOne(new Query(criteria1), MetadataDefinitionEntity.class);

        if (metadataDefinition == null) {
            MetadataDefinitionEntity entity = new MetadataDefinitionEntity();
            entity.setValue("default_app_api");
            entity.setItemType(Lists.of("app"));
            entity.setReadOnly(true);

            metadataDefinition = mongoTemplate.save(entity);
        }

        Criteria criteria = new Criteria();
        criteria.orOperator(Criteria.where("listtags").exists(false), Criteria.where("listtags").is(Lists.of()));
        Query query = new Query(criteria);


        Update update = Update.update("listtags", Lists.of(new Tag(metadataDefinition.getId().toHexString(), metadataDefinition.getValue())));
        mongoTemplate.updateMulti(query, update, ModulesEntity.class);
    }
}
