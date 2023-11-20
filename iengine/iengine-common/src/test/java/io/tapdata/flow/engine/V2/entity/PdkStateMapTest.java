package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.store.ttl.TTLCleanMode;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.pdk.core.api.CleanRuleModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

    @Mock
    private PersistenceStorage persistenceStorage;
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
        // Run the test
        long inputTTl = 10L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        // Verify the results
        Assert.assertNotNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));
    }

    /**
     * input ttl is 0
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule_ThrowsException() throws Exception {
        // Run the test
        long inputTTl = 0L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        Mockito.verify(persistenceStorage,Mockito.times(1)).setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING);
        // Verify the results
        Assert.assertNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));
    }

    /**
     * ttl is a negative number
     * @throws Exception
     */
    @Test
    public void testSetKeyTTLRule_ThrowsException2() throws Exception {
        // Run the test
        long inputTTl = -1L;
        String inputCondition = "condition";
        CleanRuleModel inputCleanRuleModel = CleanRuleModel.FUZZY_MATCHING;
        pdkStateMapUnderTest.setKeyTTLRule(inputTTl, inputCondition, inputCleanRuleModel);
        // Verify the results
        Assert.assertNull(PersistenceStorage.getInstance().setImapTTL(constructIMap.getiMap(), inputTTl,inputCondition,TTLCleanMode.FUZZY_MATCHING));
    }

}
