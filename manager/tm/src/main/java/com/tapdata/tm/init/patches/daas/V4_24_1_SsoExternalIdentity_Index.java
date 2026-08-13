package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.IndexDefinition;

/**
 * Creates the unique compound index (idpEntityId, nameId) on the SsoExternalIdentity
 * collection so that a NameID from a given IdP maps to exactly one binding.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.24-1")
public class V4_24_1_SsoExternalIdentity_Index extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_24_1_SsoExternalIdentity_Index.class);

    public V4_24_1_SsoExternalIdentity_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "SsoExternalIdentity";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        CompoundIndexDefinition compoundIndex = new CompoundIndexDefinition(
                new Document().append("idpEntityId", 1).append("nameId", 1));
        compoundIndex.unique();
        IndexDefinition index = compoundIndex.named("unq_sso_idp_nameId");
        mongoTemplate.indexOps(collectionName).createIndex(index);
    }
}
