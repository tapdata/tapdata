package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.store.ttl.TTLCleanMode;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.pdk.core.api.CleanRuleModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class PdkStateMapTest {


    private PdkStateMap pdkStateMapUnderTest;
    private DocumentIMap<Document> constructIMap;
    @Mock
    private ExternalStorageDto tapdataOrDefaultExternalStorage;
    @Mock
    private static PersistenceStorage persistenceStorage;
    private HazelcastInstance hazelcastInstance;

    @Before
    public void setUp() {

    }

    /**
     * Normal input, ttl is not 0 and negative number
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule() throws Exception {
        // Setup
        // Run the test
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);
        config.setInstanceName("test");
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        pdkStateMapUnderTest = new PdkStateMap();
        constructIMap = new DocumentIMap<>(hazelcastInstance,"","test");
        ReflectionTestUtils.setField(pdkStateMapUnderTest,"constructIMap",constructIMap);
        PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "test")
                .uri("mongodb://root:Gotapd8!@139.198.127.204:32550/qa?authSource=admin")
                .database("hazelcast")
                .collection("imap_default_config");
        Logger logger = LogManager.getLogger(this);
        persistenceStorage = PersistenceStorage.getInstance().logger(logger).addConfig(imapMongoDBConfig);
        pdkStateMapUnderTest.setKeyTTLRule(10L, "condition", CleanRuleModel.FUZZY_MATCHING);

        // Verify the results
    }

    /**
     * input ttl is 0
     * @throws Exception
     */
    @Test(expected = NullPointerException.class)
    public void testSetKeyTTLRule_ThrowsException() throws Exception {
        // Setup
        // Run the test
        pdkStateMapUnderTest.setKeyTTLRule(0L, "condition", CleanRuleModel.FUZZY_MATCHING);
    }

    /**
     * ttl is a negative number
     * @throws Exception
     */
    @Test(expected = NullPointerException.class)
    public void testSetKeyTTLRule_ThrowsException2() throws Exception {
        // Setup
        // Run the test
        pdkStateMapUnderTest.setKeyTTLRule(-1L, "condition", CleanRuleModel.FUZZY_MATCHING);
    }

}
