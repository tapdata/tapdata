package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
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
import org.junit.Assert;
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
    private HazelcastInstance hazelcastInstance;
    @Mock
    private ClientContext context;
    @Before
    public void setUp() {
        pdkStateMapUnderTest = new PdkStateMap();
        PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "test")
                .uri("mongodb://test.com")
                .database("hazelcast")
                .collection("imap_default_config");
        Logger logger = LogManager.getLogger(this);
        PersistenceStorage.getInstance().logger(logger).addConfig(imapMongoDBConfig);
        constructIMap = new DocumentIMap<>(hazelcastInstance,"123","test");
        ReflectionTestUtils.setField(constructIMap,"iMap",new ClientMapProxy("test","test",context));
        ReflectionTestUtils.setField(pdkStateMapUnderTest,"constructIMap",constructIMap);

    }

    /**
     * Normal input, ttl is not 0 and negative number
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule() throws Exception {
        // Setup
        // Run the test
        //hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        long inputTTl = 10L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        Assert.assertNotNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));


        // Verify the results
    }

    /**
     * input ttl is 0
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule_ThrowsException() throws Exception {
        // Setup
        // Run the test
        long inputTTl = 0L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        Assert.assertNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));
    }

    /**
     * ttl is a negative number
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule_ThrowsException2() throws Exception {
        // Setup
        // Run the test
        long inputTTl = -1L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        Assert.assertNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));
    }

}
