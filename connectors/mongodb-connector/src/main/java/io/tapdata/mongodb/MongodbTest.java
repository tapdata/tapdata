package io.tapdata.mongodb;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapdata.common.CommonDbTest;
import io.tapdata.constant.DbTestItem;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

public class MongodbTest extends CommonDbTest {

    private MongodbConfig mongodbConfig;
    private MongoClient mongoClient;

    public MongodbTest(MongodbConfig mongodbConfig, Consumer<TestItem> consumer, MongoClient mongoClient) {
        super(mongodbConfig, consumer);
        this.mongodbConfig = mongodbConfig;
        this.mongoClient = mongoClient;
    }

    @Override
    protected List<String> supportVersions() {
        return Arrays.asList("3.2", "3.4", "3.6", "4.0", "4.2");
    }

    public Boolean testOneByOne() {
        for (Map.Entry<String, Supplier<Boolean>> entry : testFunctionMap.entrySet()) {
            Boolean res = entry.getValue().get();
            if (EmptyKit.isNotNull(res) && !res) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean testHostPort() {
        StringBuilder failHosts;
        List<String> hosts = mongodbConfig.getHosts();
        failHosts = new StringBuilder();
        for (String host : hosts) {
            String[] split = host.split(":");
            String hostname = split[0];
            Integer port = 27017;
            if (split.length > 1) {
                port = Integer.valueOf(split[1]);
            }
            try {
                NetUtil.validateHostPortWithSocket(hostname, port);
            }catch (Exception e){
                failHosts.append(host).append(",");
            }
        }
        if (EmptyKit.isNotBlank(String.valueOf(failHosts))) {
            failHosts = new StringBuilder(failHosts.substring(0, failHosts.length() - 1));
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED,JSONObject.toJSONString(failHosts)));
            return false;
        }
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    @Override
    public Boolean testConnect() {
        try {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
            try (final MongoCursor<String> mongoCursor = mongoDatabase.listCollectionNames().iterator()) {
                mongoCursor.hasNext();
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }
    @Override
    public Boolean testVersion() {
         try{
            String version = MongodbUtil.getVersionString(mongoClient,mongodbConfig.getDatabase());
            String versionMsg ="mongodb version:"+ version;
            if (supportVersions().stream().noneMatch(v -> {
                String reg = v.replaceAll("\\*", ".*");
                Pattern pattern = Pattern.compile(reg);
                return pattern.matcher(version).matches();
            })) {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, versionMsg));
            }
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }
}
