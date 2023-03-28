package io.tapdata.mongodb.atlasTest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapdata.mongodb.MongodbTest;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import org.bson.Document;

import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Author:Skeet
 * Date: 2023/3/27
 **/
public class MongodbAtlasTest extends MongodbTest {
    public MongodbAtlasTest(MongodbConfig mongodbConfig, Consumer<TestItem> consumer, MongoClient mongoClient) {
        super(mongodbConfig, consumer, mongoClient);
        testFunctionMap.remove("testHostPort");
    }

    @Override
    public Boolean testReadPrivilege() {
        if (!isOpenAuth()) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }
        String database = mongodbConfig.getDatabase();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        Document connectionStatus = mongoDatabase.runCommand(new Document("connectionStatus", 1).append("showPrivileges", 1));
        Document isMaster = mongoDatabase.runCommand(new Document("isMaster", 1));

        if (isMaster.containsKey("msg") && "isdbgrid".equals(isMaster.getString("msg"))) {
            // validate mongos config.shards and source DB privileges
            return validateMongodb(connectionStatus);
        } else if (isMaster.containsKey("setName")) {   // validate replica set
            if (!validateAuthDB(connectionStatus)) {
                return false;
            }
            if (!validateReadOrWriteDatabase(connectionStatus, database, READ_PRIVILEGE_ACTIONS)) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "Missing read privileges on" + mongodbConfig.getDatabase() + "database"));
                return false;
            }
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } else {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Source mongodb instance must be the shards or replica set."));
            return false;
        }
    }
}