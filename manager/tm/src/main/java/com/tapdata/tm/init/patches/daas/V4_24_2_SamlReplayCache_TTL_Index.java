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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;

import java.util.concurrent.TimeUnit;

/**
 * Creates the SAML replay-cache indexes on the {@code SamlReplayCache} collection:
 * a unique compound index on {@code (type, recordId)} that makes a duplicate insert
 * (a replayed message) fail, and a TTL index on {@code createdAt} so consumed
 * records expire automatically. The TTL comfortably exceeds any reasonable
 * assertion lifetime plus clock skew.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.24-2")
public class V4_24_2_SamlReplayCache_TTL_Index extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_24_2_SamlReplayCache_TTL_Index.class);

    public V4_24_2_SamlReplayCache_TTL_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "SamlReplayCache";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        CompoundIndexDefinition compoundIndex = new CompoundIndexDefinition(
                new Document().append("type", 1).append("recordId", 1));
        compoundIndex.unique();
        IndexDefinition uniqueIndex = compoundIndex.named("unq_saml_replay_type_record");
        mongoTemplate.indexOps(collectionName).createIndex(uniqueIndex);

        long ttlValue = TimeUnit.HOURS.toSeconds(1L);
        Index ttlIndex = new Index().on("createdAt", Sort.Direction.DESC)
                .expire(ttlValue, TimeUnit.SECONDS).background();
        mongoTemplate.indexOps(collectionName).ensureIndex(ttlIndex);
    }
}
