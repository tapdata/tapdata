package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.proxyprocess.service.impl.OracleLogParserAgentProcessProjectionService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates the {@code AgentProcess} indexes and then synchronizes the latest historical
 * OracleLogParser states into that collection.
 *
 * <p>The entity only carries {@code @Indexed} annotations, and neither the application nor any
 * patch ever executes them, so the collection would keep the {@code _id} index alone. The
 * {@code (agentId, agentType)} compound unique index is the projection key:
 * <ul>
 *   <li>{@code AgentProcessRepository.upsertOracleLogParser} matches on {@code agentId} and
 *       {@code agentType} and relies on it for the {@code DuplicateKeyException} branch that
 *       keeps a stale report from replacing a newer one.</li>
 *   <li>{@code AgentProcessServiceImpl.register} looks the row up by
 *       {@code agentId} and {@code agentType} and assumes a single row per pair.</li>
 * </ul>
 *
 * <p>The index is created before the backfill on purpose: the backfill writes through the same
 * upsert, so it must not be able to insert a second document for an existing key.
 *
 * <p>The {@code ServerState} backfill input is already unique per service id: it is grouped by
 * {@code serviceId} and only the latest report of each group is projected.
 */
@Slf4j
@PatchAnnotation(appType = AppType.DAAS, version = "4.26-1")
public class V4_26_1_OracleLogParserAgentProcessSync extends AbsPatch {
    static final String COLLECTION_NAME = "AgentProcess";
    static final String AGENT_ID = "agentId";
    static final String AGENT_TYPE = "agentType";
    static final String UNIQUE_AGENT_ID_AGENT_TYPE_INDEX = "unq_agent_process_agent_id_agent_type";

    public V4_26_1_OracleLogParserAgentProcessSync(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        log.info("Execute java patch: {}...", getClass().getName());
        createAgentProcessIndexes(SpringContextHelper.getBean(MongoTemplate.class));

        log.info("Synchronize historical OracleLogParser states into AgentProcess");
        OracleLogParserAgentProcessProjectionService projectionService = SpringContextHelper
                .getBean(OracleLogParserAgentProcessProjectionService.class);
        int projected = projectionService.initializeLatestReports();
        log.info("Synchronized {} OracleLogParser states into AgentProcess", projected);
    }

    /**
     * Create the {@code (agentId, agentType)} compound unique index.
     *
     * <p>Written explicitly because {@code autoIndexCreation} stays off here: the application
     * excludes {@code DataMongoAutoConfiguration} and {@code DefaultMongoConfig} does not override
     * it, and {@code StartupListener} only applies the indexes of two other entities. Changing the
     * property in configuration would not help - the class that reads it is excluded.
     *
     * <p>An existing index with the same name but a different definition is dropped and recreated,
     * so a previously created non-unique index cannot outlive this patch.
     *
     * <p>The unique index is not guarded by a duplicate cleanup: the caller's data contract is one
     * document per agent id and agent type. A leftover duplicate therefore fails the patch loudly
     * instead of silently leaving the collection without the index this design depends on.
     */
    static void createAgentProcessIndexes(MongoTemplate mongoTemplate) {
        Document key = new Document(AGENT_ID, 1).append(AGENT_TYPE, 1);
        ensureIndex(mongoTemplate, UNIQUE_AGENT_ID_AGENT_TYPE_INDEX, key,
                new IndexOptions().name(UNIQUE_AGENT_ID_AGENT_TYPE_INDEX).unique(true).background(true));
    }

    private static void ensureIndex(MongoTemplate mongoTemplate, String name, Document key,
                                    IndexOptions options) {
        var collection = mongoTemplate.getCollection(COLLECTION_NAME);
        Document existing = collection.listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> name.equals(index.getString("name")))
                .findFirst()
                .orElse(null);
        /*
         * Compare the key manually: Document#equals ignores field order, but the order is part of
         * the index definition. A (agentType, agentId) index does not serve an agentId prefix
         * query, and an agentType-only query only reaches a (agentId, agentType) index through
         * an in-memory filter. Treating the two as equal would leave the wrong index in place.
         */
        if (existing != null && sameKeyOrder(key, existing.get("key"))
                && existing.getBoolean("unique", false)) {
            return;
        }
        if (existing != null) {
            collection.dropIndex(name);
        }
        collection.createIndex(key, options);
        log.info("Created unique index {} on {}: {}", name, COLLECTION_NAME, key);
    }

    /**
     * Whether the stored index key has the expected fields in the expected order and directions.
     *
     * <p>The index listing returns field values as the BSON numbers they were created with, which
     * {@link Document#get(Object)} yields as {@code Integer} after a driver round trip. Compare the
     * numeric direction by value rather than by boxed type.
     */
    static boolean sameKeyOrder(Document expected, Object actual) {
        if (!(actual instanceof Document actualKey) || actualKey.size() != expected.size()) {
            return false;
        }
        List<String> expectedFields = new ArrayList<>(expected.keySet());
        List<String> actualFields = new ArrayList<>(actualKey.keySet());
        if (!expectedFields.equals(actualFields)) {
            return false;
        }
        for (String field : expectedFields) {
            Object expectedDirection = expected.get(field);
            Object actualDirection = actualKey.get(field);
            if (!(expectedDirection instanceof Number expectedNumber)
                    || !(actualDirection instanceof Number actualNumber)
                    || expectedNumber.intValue() != actualNumber.intValue()) {
                return false;
            }
        }
        return true;
    }

    static List<String> indexNames() {
        return List.of(UNIQUE_AGENT_ID_AGENT_TYPE_INDEX);
    }
}
