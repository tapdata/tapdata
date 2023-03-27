package io.tapdata.mongodb;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.mongodb.atlasTest.MongodbAtlasTest;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/3/24
 **/
@TapConnectorClass("atlas-spec.json")
public class MongodbAtlasConnector extends MongodbConnector {

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try {
            onStart(connectionContext);
            try (
                    MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongoConfig, consumer,mongoClient)
            ) {
                mongodbAtlasTest.testOneByOne();
            }
        } catch (Throwable throwable) {
            TapLogger.error(TAG,throwable.getMessage());
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        } finally {
            onStop(connectionContext);
        }
        return connectionOptions;
    }
}
