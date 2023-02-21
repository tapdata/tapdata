package com.tapdata.tm.datasource;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClientFactory;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.config.MongoDbFactoryParser;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/9 上午11:17
 */
public class TestEncryptDatasourceConfig {

    @Test
    public void testEncryptConfig() {
        MongoClient client = MongoClients.create("mongodb://localhost:27017/test");
        MongoTemplate operations = new MongoTemplate(client, "test");
        DataSourceRepository repository = new DataSourceRepository(operations);

        Map<String, Object> map = new HashMap<>();
        map.put("Test1", "Test1");
        map.put("Test2", "Test2");
        map.put("Test3", "Test3");
        map.put("Test4", new HashMap<String, String>(){{
            put("Test1", "test1");
            put("Test2", "test2");
        }});

        DataSourceEntity dse = new DataSourceEntity();
        dse.setConfig(map);

        // encrypt config
        repository.encryptConfig(dse);
        Assertions.assertNull(dse.getConfig());
        Assertions.assertNotNull(dse.getEncryptConfig());

        // decrypt config
        repository.decryptConfig(dse);
        Assertions.assertNotNull(dse.getConfig());
        Assertions.assertNull(dse.getEncryptConfig());
        Map<String,Object> config = dse.getConfig();
        Assertions.assertEquals(config.get("Test1"), map.get("Test1"));
        Assertions.assertEquals(config.get("Test2"), map.get("Test2"));
        Assertions.assertEquals(config.get("Test3"), map.get("Test3"));
    }

}
